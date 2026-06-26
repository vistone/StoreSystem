use crate::logger::{LogCategory, LogEntry, LogLevel, LogStore};
use crate::pending_store::PendingStore;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

/// Master 端日志 WebSocket 服务
///
/// 接收所有 Worker 通过 WebSocket 推送的日志，
/// 写入 LogStore（SQLite 持久化）。
///
/// 日志量可能很大，因此：
/// - 使用批量写入（事务）
/// - 异步处理，不阻塞主流程
/// - 支持高并发连接
///
/// 同时承载配置推送通道：Worker 连接后，Master 可通过 ConfigBroadcaster
/// 向特定 Worker 推送配置更新消息。
pub struct MasterLogWsServer {
    store: Arc<LogStore>,
    port: u16,
    /// Pending 缓存（用于 Worker 恢复时写回）
    pending_store: Arc<PendingStore>,
    /// 配置推送器（共享给 Master 用于配置变更时下发）
    config_broadcaster: Arc<ConfigBroadcaster>,
}

impl MasterLogWsServer {
    pub fn new(store: LogStore, port: u16, pending_store: Arc<PendingStore>) -> Self {
        Self {
            store: Arc::new(store),
            port,
            pending_store,
            config_broadcaster: Arc::new(ConfigBroadcaster::new()),
        }
    }

    /// 返回 ConfigBroadcaster 的引用（供 Master 持有，用于配置推送）
    pub fn config_broadcaster(&self) -> Arc<ConfigBroadcaster> {
        self.config_broadcaster.clone()
    }

    /// 启动日志 WebSocket 服务
    pub async fn start(&self) {
        let addr = format!("0.0.0.0:{}", self.port);
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("[Master Log WS] 绑定端口 {} 失败: {}", self.port, e);
                return;
            }
        };

        println!("📋 Master Log WebSocket server running on ws://{}", addr);

        let store = self.store.clone();
        let broadcaster = self.config_broadcaster.clone();
        let pending_store = self.pending_store.clone();

        while let Ok((stream, peer)) = listener.accept().await {
            let store = store.clone();
            let broadcaster = broadcaster.clone();
            let pending_store = pending_store.clone();
            tokio::spawn(async move {
                match accept_async(stream).await {
                    Ok(ws_stream) => {
                        println!("[Master Log WS] 新连接: {}", peer);
                        handle_connection(ws_stream, store, broadcaster, pending_store).await;
                    }
                    Err(e) => {
                        eprintln!("[Master Log WS] 接受连接失败: {}", e);
                    }
                }
            });
        }
    }
}

/// 处理单个 WebSocket 连接
///
/// 同时处理三个方向的流量：
/// - 读：Worker 推送的日志消息 / pending_pull / pending_ack
/// - 写：Master 的日志 ACK + 配置更新推送 + pending_entry 流
async fn handle_connection(
    stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    store: Arc<LogStore>,
    broadcaster: Arc<ConfigBroadcaster>,
    pending_store: Arc<PendingStore>,
) {
    let (mut write, mut read) = stream.split();

    // 配置推送通道：ConfigBroadcaster 写入 → 此任务读出 → 发往 Worker
    let (config_tx, mut config_rx) = mpsc::unbounded_channel::<Message>();
    let mut worker_id_registered: Option<String> = None;

    loop {
        tokio::select! {
            // 处理 Worker → Master 的日志消息
            msg_result = read.next() => {
                match msg_result {
                    Some(Ok(Message::Text(text))) => {
                        // 解析 JSON 获取 action/type
                        let v: serde_json::Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");
                        let action = v.get("action").and_then(|a| a.as_str()).unwrap_or("");

                        // === pending_pull: Worker 请求拉取 pending 数据 ===
                        if msg_type == "pending_pull" {
                            let worker_region = v.get("region").and_then(|r| r.as_str()).unwrap_or("0");
                            handle_pending_pull(&mut write, &pending_store, worker_region).await;
                            continue;
                        }

                        // === pending_ack: Worker 确认单条 pending 已写入 ===
                        if msg_type == "pending_ack" {
                            if let (Some(ack_key), Some(ack_status)) = (
                                v.get("key").and_then(|k| k.as_str()),
                                v.get("status").and_then(|s| s.as_str()),
                            ) {
                                let worker_region = v.get("region").and_then(|r| r.as_str()).unwrap_or("0");
                                if ack_status == "ok" {
                                    let _ = pending_store.mark_done(worker_region, ack_key);
                                } else {
                                    let _ = pending_store.revert_to_pending(worker_region, ack_key);
                                }
                            }
                            continue;
                        }

                        // === register: 首条注册消息 ===
                        if worker_id_registered.is_none() && action == "register" {
                            if let Some(wid) = v.get("worker_id").and_then(|w| w.as_str()) {
                                let wid = wid.to_string();
                                broadcaster.register(wid.clone(), config_tx.clone());
                                worker_id_registered = Some(wid);
                                let ack = r#"{"status":"ok","message":"registered"}"#;
                                if write.send(Message::Text(ack.to_string())).await.is_err() {
                                    break;
                                }
                                continue;
                            }
                        }

                        // === 日志消息（默认） ===
                        let response = process_log_message(&text, &store).await;
                        let response_text = serde_json::to_string(&response).unwrap_or_default();
                        if write.send(Message::Text(response_text)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Binary(data))) => {
                        if let Ok(text) = String::from_utf8(data.to_vec()) {
                            let response = process_log_message(&text, &store).await;
                            let response_text = serde_json::to_string(&response).unwrap_or_default();
                            if write.send(Message::Text(response_text)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        if write.send(Message::Pong(p)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) | Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Err(e)) => {
                        eprintln!("[Master Log WS] 连接错误: {}", e);
                        break;
                    }
                }
            }
            // 处理 Master → Worker 的配置推送
            config_msg = config_rx.recv() => {
                if let Some(msg) = config_msg {
                    if write.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    // 连接关闭时注销
    if let Some(wid) = worker_id_registered {
        broadcaster.unregister(&wid);
    }
}

/// 处理 pending_pull 请求：从 PendingStore 流式推送 pending 条目到 Worker
async fn handle_pending_pull(
    write: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        Message,
    >,
    pending_store: &PendingStore,
    region: &str,
) {
    use base64::Engine;

    let entries = match pending_store.list_by_status(region, &["pending", "flushing"]) {
        Ok(e) => e,
        Err(_) => {
            let end = r#"{"type":"pending_end","total":0,"error":"list failed"}"#;
            let _ = write.send(Message::Text(end.to_string())).await;
            return;
        }
    };

    let total = entries.len();
    for (seq, entry) in entries.iter().enumerate() {
        // 标记为 flushing
        let _ = pending_store.mark_flushing(region, &entry.key);

        // 读取 value
        let value = match pending_store.get(region, &entry.key) {
            Ok(Some(v)) => v,
            _ => continue,
        };

        let b64 = base64::engine::general_purpose::STANDARD.encode(&value);
        let msg = serde_json::json!({
            "type": "pending_entry",
            "key": entry.key,
            "value": b64,
            "seq": seq + 1,
        });

        if write.send(Message::Text(msg.to_string())).await.is_err() {
            // 推送中断，已标记为 flushing 的条目会由 GC 超时回退
            return;
        }
    }

    let end = serde_json::json!({
        "type": "pending_end",
        "total": total,
    });
    let _ = write.send(Message::Text(end.to_string())).await;
}

/// 日志消息响应
#[derive(serde::Serialize)]
struct LogWsResponse {
    status: String,
    message: String,
    count: i64,
}

/// 处理日志消息
async fn process_log_message(text: &str, store: &LogStore) -> LogWsResponse {
    let msg: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            return LogWsResponse {
                status: "error".to_string(),
                message: format!("无效的 JSON: {}", e),
                count: 0,
            };
        }
    };

    let action = msg["action"].as_str().unwrap_or("");

    match action {
        "log_batch" => {
            // 批量日志推送
            let payload = &msg["payload"];
            let worker_id = payload["worker_id"].as_str().unwrap_or("unknown");
            let entries_val = payload["entries"].as_array();

            if let Some(entries) = entries_val {
                let log_entries: Vec<LogEntry> = entries
                    .iter()
                    .map(|e| {
                        let level_str = e["level"].as_str().unwrap_or("info");
                        let category_str = e["category"].as_str().unwrap_or("custom");
                        let timestamp_str = e["timestamp"].as_str().unwrap_or("");

                        let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp_str)
                            .map(|dt| dt.with_timezone(&chrono::Utc))
                            .unwrap_or_else(|_| chrono::Utc::now());

                        LogEntry {
                            id: 0,
                            worker_id: worker_id.to_string(),
                            level: LogLevel::from(level_str),
                            category: LogCategory::from(category_str),
                            message: e["message"].as_str().unwrap_or("").to_string(),
                            detail_json: e["detail_json"].as_str().map(|s| s.to_string()),
                            timestamp,
                            acknowledged: false,
                        }
                    })
                    .collect();

                match store.write_logs_batch(&log_entries) {
                    Ok(count) => LogWsResponse {
                        status: "ok".to_string(),
                        message: format!("已接收 {} 条日志", count),
                        count,
                    },
                    Err(e) => LogWsResponse {
                        status: "error".to_string(),
                        message: format!("写入日志失败: {}", e),
                        count: 0,
                    },
                }
            } else {
                LogWsResponse {
                    status: "error".to_string(),
                    message: "缺少 entries 字段".to_string(),
                    count: 0,
                }
            }
        }
        "log_single" => {
            // 单条日志推送
            let payload = &msg["payload"];
            let worker_id = payload["worker_id"].as_str().unwrap_or("unknown");
            let level_str = payload["level"].as_str().unwrap_or("info");
            let category_str = payload["category"].as_str().unwrap_or("custom");

            match store.log(
                worker_id,
                LogLevel::from(level_str),
                LogCategory::from(category_str),
                payload["message"].as_str().unwrap_or(""),
                payload["detail_json"].as_str(),
            ) {
                Ok(_) => LogWsResponse {
                    status: "ok".to_string(),
                    message: "日志已接收".to_string(),
                    count: 1,
                },
                Err(e) => LogWsResponse {
                    status: "error".to_string(),
                    message: format!("写入日志失败: {}", e),
                    count: 0,
                },
            }
        }
        _ => LogWsResponse {
            status: "error".to_string(),
            message: format!("未知 action: {}", action),
            count: 0,
        },
    }
}

// ============================================================
// ConfigBroadcaster - 配置推送器
// ============================================================

use dashmap::DashMap;

/// 配置推送器：维护 worker_id → WS sender 的映射
///
/// Master 在配置变更时调用 `broadcast_config_update`，
/// 通过日志 WS 连接的反向通道推送 config_update 消息给 Worker。
pub struct ConfigBroadcaster {
    /// worker_id → mpsc sender（sender 写入的消息会被 WS 写任务发往 Worker）
    senders: DashMap<String, mpsc::UnboundedSender<Message>>,
}

impl Default for ConfigBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigBroadcaster {
    pub fn new() -> Self {
        Self {
            senders: DashMap::new(),
        }
    }

    /// 注册 Worker 的 WS sender（Worker 连接日志 WS 时调用）
    pub fn register(&self, worker_id: String, tx: mpsc::UnboundedSender<Message>) {
        self.senders.insert(worker_id, tx);
    }

    /// 注销 Worker（连接关闭时调用）
    pub fn unregister(&self, worker_id: &str) {
        self.senders.remove(worker_id);
    }

    /// 向指定 Worker 推送配置更新
    pub fn broadcast_config_update(&self, worker_id: &str, config_json: &str) {
        if let Some(tx) = self.senders.get(worker_id) {
            let _ = tx.send(Message::Text(config_json.to_string()));
        }
    }

    /// 向所有 Worker 广播配置更新
    pub fn broadcast_all(&self, config_json: &str) {
        for entry in self.senders.iter() {
            let _ = entry.value().send(Message::Text(config_json.to_string()));
        }
    }

    /// 当前注册的 Worker 数量
    pub fn len(&self) -> usize {
        self.senders.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.senders.is_empty()
    }
}

impl std::fmt::Debug for ConfigBroadcaster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigBroadcaster")
            .field("registered_workers", &self.senders.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_broadcaster_register_and_broadcast() {
        let broadcaster = ConfigBroadcaster::new();
        let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
        broadcaster.register("worker-0".to_string(), tx);

        assert_eq!(broadcaster.len(), 1);

        broadcaster
            .broadcast_config_update("worker-0", r#"{"type":"config_update","config_version":2}"#);

        let msg = rx.recv().await.expect("should receive message");
        match msg {
            Message::Text(t) => {
                assert!(t.contains("config_update"));
                assert!(t.contains("\"config_version\":2"));
            }
            _ => panic!("expected Text message"),
        }
    }

    #[tokio::test]
    async fn test_config_broadcaster_unregister() {
        let broadcaster = ConfigBroadcaster::new();
        let (tx, _rx) = mpsc::unbounded_channel::<Message>();
        broadcaster.register("worker-0".to_string(), tx);
        assert_eq!(broadcaster.len(), 1);

        broadcaster.unregister("worker-0");
        assert_eq!(broadcaster.len(), 0);
        assert!(broadcaster.is_empty());

        // 注销后广播不应 panic
        broadcaster.broadcast_config_update("worker-0", "{}");
    }

    #[tokio::test]
    async fn test_config_broadcaster_broadcast_all() {
        let broadcaster = ConfigBroadcaster::new();
        let (tx1, mut rx1) = mpsc::unbounded_channel::<Message>();
        let (tx2, mut rx2) = mpsc::unbounded_channel::<Message>();
        broadcaster.register("worker-0".to_string(), tx1);
        broadcaster.register("worker-1".to_string(), tx2);

        broadcaster.broadcast_all(r#"{"type":"config_update"}"#);

        let m1 = rx1.recv().await.expect("worker-0 should receive");
        let m2 = rx2.recv().await.expect("worker-1 should receive");
        assert!(m1.to_text().unwrap().contains("config_update"));
        assert!(m2.to_text().unwrap().contains("config_update"));
    }

    #[tokio::test]
    async fn test_config_broadcaster_unknown_worker_no_panic() {
        let broadcaster = ConfigBroadcaster::new();
        // 向未注册的 worker 广播不应 panic
        broadcaster.broadcast_config_update("worker-99", "{}");
    }
}
