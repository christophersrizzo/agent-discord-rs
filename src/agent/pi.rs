use super::{AgentEvent, AgentState, AiAgent, ContentItem, ContentType, ModelInfo};
use crate::agent::runtime;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, Command};
use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tracing::{info, warn};

pub struct PiAgent {
    stdin: Arc<Mutex<ChildStdin>>,
    event_tx: broadcast::Sender<AgentEvent>,
    child_pid: u32,
    _pending_trace: Arc<Mutex<String>>, // 修改為非 Option，方便狀態機追加
}

impl PiAgent {
    pub async fn new(channel_id: u64, session_dir: &PathBuf) -> anyhow::Result<(Arc<Self>, u64)> {
        std::fs::create_dir_all(session_dir)?;
        let pi_binary = runtime::resolve_binary_with_env("PI_BINARY", "pi");
        let current_path = std::env::var("PATH").unwrap_or_default();
        let augmented_path = runtime::build_augmented_path(&current_path);

        info!("🚀 Spawning Pi binary: {}", pi_binary);
        let session_file = session_dir.join(format!("discord-rs-{}.jsonl", channel_id));
        let mut child = Command::new(&pi_binary)
            .arg("--mode")
            .arg("rpc")
            .arg("--session")
            .arg(&session_file)
            .arg("--session-dir")
            .arg(session_dir)
            .env("PATH", augmented_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let child_pid = child.id().unwrap_or(0);
        let stdin = Arc::new(Mutex::new(child.stdin.take().unwrap()));
        // NOTE: tool-heavy runs can generate a lot of small events; a larger buffer
        // reduces the chance the Discord-side writer lags and drops the final ContentSync.
        let (event_tx, _) = broadcast::channel(5000);
        let tx = event_tx.clone();
        let pending_trace = Arc::new(Mutex::new(String::new()));

        let stdout = child.stdout.take().unwrap();
        let tx_stdout = tx.clone();
        let trace_stdout = pending_trace.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            while let Ok(n) = reader.read_line(&mut line).await {
                if n == 0 {
                    break;
                }
                if let Ok(val) = serde_json::from_str::<Value>(line.trim()) {
                    Self::parse_event(&tx_stdout, val, &trace_stdout).await;
                }
                line.clear();
            }
        });

        let stderr = child.stderr.take().unwrap();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut line = String::new();
            while let Ok(n) = reader.read_line(&mut line).await {
                if n == 0 {
                    break;
                }
                let msg = line.trim();
                if !msg.is_empty() {
                    warn!("pi(stderr): {}", msg);
                }
                line.clear();
            }
        });

        tokio::spawn(async move {
            let status = child.wait().await;
            info!("Pi process (PID {}) exited with {:?}", child_pid, status);
        });

        let agent = Arc::new(PiAgent {
            stdin,
            event_tx: tx,
            child_pid,
            _pending_trace: pending_trace,
        });
        agent
            .raw_call(
                json!({ "type": "set_session_name", "name": format!("discord-rs-{}", channel_id) }),
            )
            .await?;
        Ok((agent, 0))
    }

    async fn parse_event(
        tx: &broadcast::Sender<AgentEvent>,
        val: Value,
        trace_buf: &Arc<Mutex<String>>,
    ) {
        let type_ = val["type"].as_str().unwrap_or("");

        let is_trace_start = |s: &str| {
            let t = s.trim_start();
            t.starts_with('→') || t.starts_with("🛠️")
        };
        let is_control = |s: &str| s.trim_start().starts_with("<ctrl");

        match type_ {
            "message_update" | "text_delta" | "thinking_delta" => {
                let delta_obj = val
                    .get("assistantMessageEvent")
                    .or(val.get("message"))
                    .unwrap_or(&val);

                // 1. 處理全量同步
                if let Some(partial) = delta_obj
                    .get("partial")
                    .and_then(|p| p.get("content"))
                    .and_then(|c| c.as_array())
                {
                    let mut items = Vec::new();
                    let mut i = 0;
                    while i < partial.len() {
                        let item = &partial[i];
                        let t = item["type"].as_str().unwrap_or("");
                        if t == "text" {
                            let c = item["text"].as_str().unwrap_or("");
                            // 合併同步中的 Trace 與 ToolCall
                            if is_trace_start(c)
                                && i + 1 < partial.len()
                                && partial[i + 1]["type"] == "toolCall"
                            {
                                let tc = &partial[i + 1]["toolCall"];
                                items.push(ContentItem {
                                    type_: ContentType::ToolCall(c.trim().to_string()),
                                    content: String::new(),
                                    id: tc["id"].as_str().map(|s| s.to_string()),
                                });
                                i += 2;
                                continue;
                            } else if !c.is_empty() && !is_trace_start(c) && !is_control(c) {
                                items.push(ContentItem {
                                    type_: ContentType::Text,
                                    content: c.to_string(),
                                    id: None,
                                });
                            }
                        } else if t == "thinking" || t == "thought" || item.get("thought").is_some()
                        {
                            let c = item["thinking"]
                                .as_str()
                                .or(item["thought"].as_str())
                                .unwrap_or("")
                                .to_string();
                            if !c.is_empty() {
                                items.push(ContentItem {
                                    type_: ContentType::Thinking,
                                    content: c,
                                    id: None,
                                });
                            }
                        } else if t == "toolCall" {
                            let tc = &item["toolCall"];
                            items.push(ContentItem {
                                type_: ContentType::ToolCall(
                                    tc["name"].as_str().unwrap_or("tool").to_string(),
                                ),
                                content: String::new(),
                                id: tc["id"].as_str().map(|s| s.to_string()),
                            });
                        }
                        i += 1;
                    }
                    if !items.is_empty() {
                        let _ = tx.send(AgentEvent::ContentSync { items });
                        return;
                    }
                }

                // 2. 處理增量 Delta (強化狀態機邏輯)
                if let Some(d) = delta_obj.get("delta").and_then(|d| d.as_str()) {
                    let mut buf = trace_buf.lock().await;

                    // 檢查是否包含指令起始符
                    let marker_pos = d.find('→').or_else(|| d.find("🛠️"));

                    if let Some(pos) = marker_pos {
                        // 如果有起始符，切分：前半段作為普通文字發送，後半段（含起始符）進入緩衝區
                        let (text, trace) = d.split_at(pos);
                        if !text.is_empty() {
                            let _ = tx.send(AgentEvent::MessageUpdate {
                                thinking: "".to_string(),
                                text: text.to_string(),
                                is_delta: true,
                                id: None,
                            });
                        }
                        buf.push_str(trace);
                        return;
                    } else if !buf.is_empty() {
                        // 如果已經在 Trace 模式中，且當前 Delta 不包含換行符，持續攔截
                        if !d.contains('\n') || d.trim().is_empty() {
                            buf.push_str(d);
                            return;
                        } else {
                            // 如果遇到換行且不像是 Trace 續行，可能 AI 只是隨口提到了指令但沒真的用，或者指令結束了
                            // 這裡我們保守一點，如果緩衝區有東西但沒收到工具啟動，我們就在下一次同步時修正
                            buf.push_str(d);
                            return;
                        }
                    }

                    if is_control(d) {
                        return;
                    }
                    let is_thinking = type_ == "thinking_delta"
                        || delta_obj["type"]
                            .as_str()
                            .unwrap_or("")
                            .contains("thinking");
                    let _ = tx.send(AgentEvent::MessageUpdate {
                        thinking: if is_thinking {
                            d.to_string()
                        } else {
                            "".to_string()
                        },
                        text: if is_thinking {
                            "".to_string()
                        } else {
                            d.to_string()
                        },
                        is_delta: true,
                        id: None,
                    });
                }
            }
            "tool_execution_start" => {
                let id = val["toolCallId"].as_str().unwrap_or("").to_string();
                let mut buf = trace_buf.lock().await;
                // 優先使用狀態機攔截到的完整描述，若無則用原始 toolName
                let name = if !buf.is_empty() {
                    buf.split_off(0)
                } else {
                    val["toolName"].as_str().unwrap_or("tool").to_string()
                };
                let _ = tx.send(AgentEvent::ToolExecutionStart { id, name });
            }
            "tool_execution_update" => {
                let id = val["toolCallId"].as_str().unwrap_or("").to_string();
                if let Some(content) = val
                    .get("partialResult")
                    .and_then(|p| p.get("content"))
                    .and_then(|c| c.as_array())
                {
                    for item in content {
                        if let Some(output) = item["text"].as_str() {
                            let _ = tx.send(AgentEvent::ToolExecutionUpdate {
                                id: id.clone(),
                                output: output.to_string(),
                            });
                        }
                    }
                }
            }
            "tool_execution_end" => {
                let id = val["toolCallId"].as_str().unwrap_or("").to_string();
                if let Some(result) = val
                    .get("result")
                    .and_then(|r| r.get("content"))
                    .and_then(|c| c.as_array())
                {
                    for item in result {
                        if let Some(output) = item["text"].as_str() {
                            let _ = tx.send(AgentEvent::ToolExecutionUpdate {
                                id: id.clone(),
                                output: output.to_string(),
                            });
                        }
                    }
                }
                let name = val["toolName"].as_str().unwrap_or("tool").to_string();
                let _ = tx.send(AgentEvent::ToolExecutionEnd { id, name });
            }
            "agent_end" => {
                let mut final_err = None;
                if let Some(err) = val.get("errorMessage").and_then(|e| e.as_str()) {
                    final_err = Some(err.to_string());
                }
                if let Some(msgs) = val.get("messages").and_then(|m| m.as_array()) {
                    // 修正：提取最後一條使用者訊息之後的所有內容，而不只是最後一條助理訊息
                    let current_turn_start = msgs
                        .iter()
                        .rposition(|m| m["role"] == "user")
                        .map(|idx| idx + 1)
                        .unwrap_or(0);
                    let current_turn = msgs.get(current_turn_start..).unwrap_or(&[]);

                    let mut items = Vec::new();
                    for msg in current_turn {
                        let role = msg["role"].as_str().unwrap_or("");
                        if let Some(content) = msg.get("content").and_then(|c| c.as_array()) {
                            let mut i = 0;
                            while i < content.len() {
                                let item = &content[i];
                                let t = item["type"].as_str().unwrap_or("");
                                if t == "text" {
                                    let s = item["text"].as_str().unwrap_or("");
                                    if is_trace_start(s)
                                        && i + 1 < content.len()
                                        && content[i + 1]["type"] == "toolCall"
                                    {
                                        let tc = &content[i + 1]["toolCall"];
                                        items.push(ContentItem {
                                            type_: ContentType::ToolCall(s.trim().to_string()),
                                            content: "".to_string(),
                                            id: tc["id"].as_str().map(|s| s.to_string()),
                                        });
                                        i += 2;
                                        continue;
                                    } else if !s.is_empty() && !is_trace_start(s) && !is_control(s)
                                    {
                                        // 如果這是工具訊息的結果文字
                                        if role == "tool" {
                                            items.push(ContentItem {
                                                type_: ContentType::ToolOutput,
                                                content: s.to_string(),
                                                id: None,
                                            });
                                        } else {
                                            items.push(ContentItem {
                                                type_: ContentType::Text,
                                                content: s.to_string(),
                                                id: None,
                                            });
                                        }
                                    }
                                } else if t == "thinking" || item.get("thinking").is_some() {
                                    let s = item["thinking"].as_str().unwrap_or("");
                                    if !s.is_empty() {
                                        items.push(ContentItem {
                                            type_: ContentType::Thinking,
                                            content: s.to_string(),
                                            id: None,
                                        });
                                    }
                                } else if t == "toolCall" {
                                    let tc = &item["toolCall"];
                                    items.push(ContentItem {
                                        type_: ContentType::ToolCall(
                                            tc["name"].as_str().unwrap_or("tool").to_string(),
                                        ),
                                        content: "".to_string(),
                                        id: tc["id"].as_str().map(|s| s.to_string()),
                                    });
                                }
                                i += 1;
                            }
                        }

                        // 處理 errorMessage (Gemini 429 等)
                        if let Some(err) = msg.get("errorMessage").and_then(|e| e.as_str()) {
                            final_err = Some(err.to_string());
                        }
                    }
                    if !items.is_empty() {
                        let _ = tx.send(AgentEvent::ContentSync { items });
                    }
                }
                let _ = tx.send(AgentEvent::AgentEnd {
                    success: final_err.is_none(),
                    error: final_err,
                });
            }
            "response" => {
                if let Some(id) = val["id"].as_str() {
                    let _ = tx.send(AgentEvent::CommandResponse {
                        id: id.to_string(),
                        data: val["data"].clone(),
                    });
                }
            }
            "error" => {
                let _ = tx.send(AgentEvent::Error {
                    message: val["message"]
                        .as_str()
                        .or(val["error"].as_str())
                        .unwrap_or("Error")
                        .to_string(),
                });
            }
            _ => {}
        }
    }

    pub async fn raw_call(&self, mut cmd: Value) -> anyhow::Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        if let Some(obj) = cmd.as_object_mut() {
            obj.insert("id".to_string(), json!(id));
        }
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all((serde_json::to_string(&cmd)? + "\n").as_bytes())
            .await?;
        stdin.flush().await?;
        Ok(id)
    }

    fn kill_child(&self) {
        if self.child_pid > 0 {
            unsafe {
                libc::kill(self.child_pid as libc::pid_t, libc::SIGKILL);
            }
        }
    }
}

#[async_trait]
impl AiAgent for PiAgent {
    async fn prompt(&self, message: &str) -> anyhow::Result<()> {
        self.raw_call(json!({ "type": "prompt", "message": message, "stream": true, "streamingBehavior": "steer" })).await?;
        Ok(())
    }
    async fn set_session_name(&self, name: &str) -> anyhow::Result<()> {
        self.raw_call(json!({ "type": "set_session_name", "name": name }))
            .await?;
        Ok(())
    }
    async fn get_state(&self) -> anyhow::Result<AgentState> {
        Ok(AgentState {
            message_count: 0,
            model: None,
        })
    }
    async fn compact(&self) -> anyhow::Result<()> {
        self.raw_call(json!({ "type": "compact" })).await?;
        Ok(())
    }
    async fn abort(&self) -> anyhow::Result<()> {
        self.raw_call(json!({ "type": "abort" })).await?;
        Ok(())
    }
    async fn clear(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn set_model(&self, p: &str, mid: &str) -> anyhow::Result<()> {
        self.raw_call(json!({ "type": "set_model", "provider": p, "modelId": mid }))
            .await?;
        Ok(())
    }
    async fn set_thinking_level(&self, l: &str) -> anyhow::Result<()> {
        self.raw_call(json!({ "type": "set_thinking_level", "level": l }))
            .await?;
        Ok(())
    }
    async fn get_available_models(&self) -> anyhow::Result<Vec<ModelInfo>> {
        let id = self
            .raw_call(json!({ "type": "get_available_models" }))
            .await?;
        let mut rx = self.event_tx.subscribe();
        let result = tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            loop {
                match rx.recv().await {
                    Ok(AgentEvent::CommandResponse { id: rid, data }) if rid == id => {
                        let models = data["models"]
                            .as_array()
                            .ok_or_else(|| anyhow::anyhow!("Missing models array"))?;
                        return Ok(models
                            .iter()
                            .filter_map(|m| {
                                Some(ModelInfo {
                                    provider: m["provider"].as_str()?.to_string(),
                                    id: m["id"].as_str()?.to_string(),
                                    label: format!(
                                        "{}/{}",
                                        m["provider"].as_str()?,
                                        m["id"].as_str()?
                                    ),
                                })
                            })
                            .collect());
                    }
                    _ => continue,
                }
            }
        })
        .await;
        result.unwrap_or(Err(anyhow::anyhow!("Timeout")))
    }
    async fn load_skill(&self, n: &str) -> anyhow::Result<()> {
        self.raw_call(json!({ "type": "load_skill", "name": n }))
            .await?;
        Ok(())
    }
    fn subscribe_events(&self) -> broadcast::Receiver<AgentEvent> {
        self.event_tx.subscribe()
    }
    fn agent_type(&self) -> &'static str {
        "pi"
    }
}

impl Drop for PiAgent {
    fn drop(&mut self) {
        self.kill_child();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_parser_test() -> (
        broadcast::Sender<AgentEvent>,
        broadcast::Receiver<AgentEvent>,
        Arc<Mutex<String>>,
    ) {
        let (tx, rx) = broadcast::channel(10);
        let pending = Arc::new(Mutex::new(String::new()));
        (tx, rx, pending)
    }

    #[tokio::test]
    async fn test_parse_event_text_delta() {
        let (tx, mut rx, pending) = setup_parser_test();
        let val = json!({
            "type": "text_delta",
            "delta": "hello "
        });
        PiAgent::parse_event(&tx, val, &pending).await;
        if let AgentEvent::MessageUpdate { text, is_delta, .. } = rx.recv().await.unwrap() {
            assert_eq!(text, "hello ");
            assert!(is_delta);
        }
    }

    #[tokio::test]
    async fn test_parse_event_thinking_delta() {
        let (tx, mut rx, pending) = setup_parser_test();
        let val = json!({
            "type": "thinking_delta",
            "delta": "deep logic"
        });
        PiAgent::parse_event(&tx, val, &pending).await;
        if let AgentEvent::MessageUpdate { thinking, .. } = rx.recv().await.unwrap() {
            assert_eq!(thinking, "deep logic");
        }
    }

    #[tokio::test]
    async fn test_parse_event_trace_interception() {
        let (tx, mut rx, pending) = setup_parser_test();

        // 模擬 AI 開始輸出 Trace 標記
        let val1 = json!({ "type": "text_delta", "delta": "I will run →" });
        PiAgent::parse_event(&tx, val1, &pending).await;

        // 預期 "I will run " 被發出，而 "→" 被攔截進 pending
        if let AgentEvent::MessageUpdate { text, .. } = rx.recv().await.unwrap() {
            assert_eq!(text, "I will run ");
        }
        assert_eq!(*pending.lock().await, "→");

        // 模擬接下來的 Delta 被攔截
        let val2 = json!({ "type": "text_delta", "delta": "ls -la" });
        PiAgent::parse_event(&tx, val2, &pending).await;
        assert_eq!(*pending.lock().await, "→ls -la");

        // 模擬工具啟動，此時應取回完整的攔截內容作為工具名稱
        let val3 = json!({ "type": "tool_execution_start", "toolCallId": "1", "toolName": "bash" });
        PiAgent::parse_event(&tx, val3, &pending).await;
        if let AgentEvent::ToolExecutionStart { name, .. } = rx.recv().await.unwrap() {
            assert_eq!(name, "→ls -la");
        }
        assert!(pending.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_parse_event_content_sync() {
        let (tx, mut rx, pending) = setup_parser_test();
        let val = json!({
            "type": "message_update",
            "partial": {
                "content": [
                    { "type": "text", "text": "Result: " },
                    { "type": "thinking", "thinking": "Done." }
                ]
            }
        });
        PiAgent::parse_event(&tx, val, &pending).await;
        if let AgentEvent::ContentSync { items } = rx.recv().await.unwrap() {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0].content, "Result: ");
            assert_eq!(items[1].type_, ContentType::Thinking);
        }
    }

    #[tokio::test]
    async fn test_parse_event_agent_end_with_empty_messages() {
        let (tx, mut rx, pending) = setup_parser_test();
        let val = json!({
            "type": "agent_end",
            "messages": []
        });
        PiAgent::parse_event(&tx, val, &pending).await;
        if let AgentEvent::AgentEnd { success, error } = rx.recv().await.unwrap() {
            assert!(success);
            assert!(error.is_none());
        } else {
            panic!("Wrong event");
        }
    }

    #[tokio::test]
    async fn test_parse_event_tool_id_extraction() {
        let (tx, mut rx, pending) = setup_parser_test();
        let val =
            json!({ "type": "tool_execution_start", "toolCallId": "id-99", "toolName": "bash" });
        PiAgent::parse_event(&tx, val, &pending).await;
        if let AgentEvent::ToolExecutionStart { id, .. } = rx.recv().await.unwrap() {
            assert_eq!(id, "id-99");
        } else {
            panic!("Wrong event");
        }
    }

    #[tokio::test]
    async fn test_parse_event_partial_trace_merges_into_tool_call() {
        let (tx, mut rx, pending) = setup_parser_test();
        let val = json!({
            "type": "message_update",
            "partial": {
                "content": [
                    { "type": "text", "text": "→ run tool" },
                    { "type": "toolCall", "toolCall": { "id": "t1", "name": "bash" } }
                ]
            }
        });
        PiAgent::parse_event(&tx, val, &pending).await;
        let ev = rx.recv().await.unwrap();
        match ev {
            AgentEvent::ContentSync { items } => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].id.as_deref(), Some("t1"));
                assert!(matches!(items[0].type_, ContentType::ToolCall(_)));
            }
            _ => panic!("expected content sync"),
        }
    }

    #[tokio::test]
    async fn test_parse_event_tool_execution_update_and_end() {
        let (tx, mut rx, pending) = setup_parser_test();
        let update = json!({
            "type":"tool_execution_update",
            "toolCallId":"tid",
            "partialResult":{"content":[{"text":"line1"},{"text":"line2"}]}
        });
        PiAgent::parse_event(&tx, update, &pending).await;
        match rx.recv().await.unwrap() {
            AgentEvent::ToolExecutionUpdate { id, output } => {
                assert_eq!(id, "tid");
                assert_eq!(output, "line1");
            }
            _ => panic!("expected tool update"),
        }
        match rx.recv().await.unwrap() {
            AgentEvent::ToolExecutionUpdate { id, output } => {
                assert_eq!(id, "tid");
                assert_eq!(output, "line2");
            }
            _ => panic!("expected tool update"),
        }

        let end = json!({
            "type":"tool_execution_end",
            "toolCallId":"tid",
            "toolName":"bash",
            "result":{"content":[{"text":"done"}]}
        });
        PiAgent::parse_event(&tx, end, &pending).await;
        match rx.recv().await.unwrap() {
            AgentEvent::ToolExecutionUpdate { id, output } => {
                assert_eq!(id, "tid");
                assert_eq!(output, "done");
            }
            _ => panic!("expected tool update"),
        }
        match rx.recv().await.unwrap() {
            AgentEvent::ToolExecutionEnd { id, name } => {
                assert_eq!(id, "tid");
                assert_eq!(name, "bash");
            }
            _ => panic!("expected tool end"),
        }
    }

    #[tokio::test]
    async fn test_parse_event_response_and_error() {
        let (tx, mut rx, pending) = setup_parser_test();
        let response = json!({"type":"response","id":"cmd-1","data":{"ok":true}});
        PiAgent::parse_event(&tx, response, &pending).await;
        match rx.recv().await.unwrap() {
            AgentEvent::CommandResponse { id, data } => {
                assert_eq!(id, "cmd-1");
                assert_eq!(data["ok"], true);
            }
            _ => panic!("expected response"),
        }

        let err = json!({"type":"error","error":"boom"});
        PiAgent::parse_event(&tx, err, &pending).await;
        match rx.recv().await.unwrap() {
            AgentEvent::Error { message } => assert_eq!(message, "boom"),
            _ => panic!("expected error"),
        }
    }

    #[tokio::test]
    async fn test_parse_event_agent_end_extracts_current_turn_items_and_error() {
        let (tx, mut rx, pending) = setup_parser_test();
        let val = json!({
            "type":"agent_end",
            "messages":[
                {"role":"assistant","content":[{"type":"text","text":"old"}]},
                {"role":"user","content":[{"type":"text","text":"question"}]},
                {"role":"tool","content":[{"type":"text","text":"tool output"}]},
                {"role":"assistant","content":[{"type":"thinking","thinking":"plan"},{"type":"text","text":"answer"}], "errorMessage":"rate limited"}
            ]
        });
        PiAgent::parse_event(&tx, val, &pending).await;

        let sync = rx.recv().await.unwrap();
        match sync {
            AgentEvent::ContentSync { items } => {
                assert!(items.iter().any(|i| i.content == "tool output"));
                assert!(items.iter().any(|i| i.content == "plan"));
                assert!(items.iter().any(|i| i.content == "answer"));
            }
            _ => panic!("expected content sync"),
        }

        match rx.recv().await.unwrap() {
            AgentEvent::AgentEnd { success, error } => {
                assert!(!success);
                assert_eq!(error.as_deref(), Some("rate limited"));
            }
            _ => panic!("expected agent end"),
        }
    }
}
