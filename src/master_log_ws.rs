use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use futures_util::StreamExt;
use futures_util::SinkExt;
use crate::logger::{LogStore, LogEntry, LogLevel, LogCategory};

/// Master 端日志 WebSocket 服务
///
/// 接收所有 Worker 通过 WebSocket 推送的日志，
/// 写入 LogStore（SQLite 持久化）。
///
/// 日志量可能很大，因此：
/// - 使用批量写入（事务）
/// - 异步处理，不阻塞主流程
/// - 支持高并发连接
pub struct MasterLogWsServer {
    store: Arc<LogStore>,
    port: u16,
}

impl MasterLogWsServer {
    pub fn new(store: LogStore, port: u16) -> Self {
        Self {
            store: Arc::new(store),
            port,
        }
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

        while let Ok((stream, peer)) = listener.accept().await {
            let store = store.clone();
            tokio::spawn(async move {
                match accept_async(stream).await {
                    Ok(ws_stream) => {
                        println!("[Master Log WS] 新日志连接: {}", peer);
                        handle_log_connection(ws_stream, store).await;
                    }
                    Err(e) => {
                        eprintln!("[Master Log WS] 接受连接失败: {}", e);
                    }
                }
            });
        }
    }
}

/// 处理单个日志 WebSocket 连接
async fn handle_log_connection(
    stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    store: Arc<LogStore>,
) {
    let (mut write, mut read) = stream.split();

    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                let response = process_log_message(&text, &store).await;
                let response_text = serde_json::to_string(&response).unwrap_or_default();
                if write.send(Message::Text(response_text.into())).await.is_err() {
                    break;
                }
            }
            Ok(Message::Binary(data)) => {
                if let Ok(text) = String::from_utf8(data.to_vec()) {
                    let response = process_log_message(&text, &store).await;
                    let response_text = serde_json::to_string(&response).unwrap_or_default();
                    if write.send(Message::Text(response_text.into())).await.is_err() {
                        break;
                    }
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                eprintln!("[Master Log WS] 连接错误: {}", e);
                break;
            }
            _ => {}
        }
    }
}

/// 日志消息响应
#[derive(serde::Serialize)]
struct LogWsResponse {
    status: String,
    message: String,
    count: usize,
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
                let log_entries: Vec<LogEntry> = entries.iter().map(|e| {
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
                }).collect();

                match store.write_logs_batch(&log_entries) {
                    Ok(count) => {
                        LogWsResponse {
                            status: "ok".to_string(),
                            message: format!("已接收 {} 条日志", count),
                            count,
                        }
                    }
                    Err(e) => {
                        LogWsResponse {
                            status: "error".to_string(),
                            message: format!("写入日志失败: {}", e),
                            count: 0,
                        }
                    }
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
                Ok(_) => {
                    LogWsResponse {
                        status: "ok".to_string(),
                        message: "日志已接收".to_string(),
                        count: 1,
                    }
                }
                Err(e) => {
                    LogWsResponse {
                        status: "error".to_string(),
                        message: format!("写入日志失败: {}", e),
                        count: 0,
                    }
                }
            }
        }
        _ => {
            LogWsResponse {
                status: "error".to_string(),
                message: format!("未知 action: {}", action),
                count: 0,
            }
        }
    }
}
