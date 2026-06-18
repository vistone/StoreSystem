use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use futures_util::StreamExt;
use crate::worker::WorkerNode;

use base64::{Engine as _, engine::general_purpose};

// ============================================================
// WebSocket 消息协议定义
// ============================================================

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "action", content = "payload")]
pub enum WsRequest {
    Put(PutPayload),
    Get(GetPayload),
    Delete(DeletePayload),
    Exists(ExistsPayload),
    List(ListPayload),
    PutBatch(PutBatchPayload),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutPayload {
    pub key: String,
    pub value: String, // base64
    pub content_type: Option<String>,
    pub tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetPayload {
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeletePayload {
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExistsPayload {
    pub key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListPayload {
    pub prefix: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchItem {
    pub key: String,
    pub value: String, // base64
    pub content_type: Option<String>,
    pub tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutBatchPayload {
    pub items: Vec<BatchItem>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status")]
pub enum WsResponse {
    #[serde(rename = "ok")]
    Ok { data: serde_json::Value },
    #[serde(rename = "error")]
    Error { message: String },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ObjectMetaResponse {
    pub key: String,
    pub size: u64,
    pub created_at: String,
    pub updated_at: String,
    pub content_type: Option<String>,
    pub tags: Option<serde_json::Value>,
}

fn convert_meta(meta: crate::meta::ObjectMeta) -> ObjectMetaResponse {
    ObjectMetaResponse {
        key: meta.key,
        size: meta.size,
        created_at: meta.created_at.to_rfc3339(),
        updated_at: meta.updated_at.to_rfc3339(),
        content_type: meta.content_type,
        tags: meta.tags,
    }
}

/// 处理单个 WebSocket 连接
async fn handle_connection(stream: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>, node: Arc<WorkerNode>) {
    let (write, mut read) = stream.split();

    // 使用一个 channel 来发送消息
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Message>();

    // 写任务
    let write_task = tokio::spawn(async move {
        use futures_util::SinkExt;
        let mut write = write;
        while let Some(msg) = rx.recv().await {
            if write.send(msg).await.is_err() {
                break;
            }
        }
    });

    // 读任务 - 处理请求
    while let Some(msg_result) = read.next().await {
        match msg_result {
            Ok(Message::Text(text)) => {
                let response = process_message(&text, &node).await;
                let response_text = serde_json::to_string(&response).unwrap_or_default();
                let _ = tx.send(Message::Text(response_text.into()));
            }
            Ok(Message::Binary(data)) => {
                // 支持二进制消息（直接作为 value 处理）
                if let Ok(text) = String::from_utf8(data.to_vec()) {
                    let response = process_message(&text, &node).await;
                    let response_text = serde_json::to_string(&response).unwrap_or_default();
                    let _ = tx.send(Message::Text(response_text.into()));
                }
            }
            Ok(Message::Close(_)) => break,
            Err(e) => {
                eprintln!("[Worker WS] 连接错误: {}", e);
                break;
            }
            _ => {}
        }
    }

    write_task.abort();
}

/// 处理 WS 消息
async fn process_message(text: &str, node: &WorkerNode) -> WsResponse {
    let request: WsRequest = match serde_json::from_str(text) {
        Ok(req) => req,
        Err(e) => {
            return WsResponse::Error {
                message: format!("无效的请求格式: {}", e),
            };
        }
    };

    match request {
        WsRequest::Put(payload) => {
            let value = match general_purpose::STANDARD.decode(&payload.value) {
                Ok(v) => Bytes::from(v),
                Err(e) => return WsResponse::Error { message: format!("Base64 解码失败: {}", e) },
            };

            let now = chrono::Utc::now();
            let meta = crate::meta::ObjectMeta {
                key: payload.key.clone(),
                size: value.len() as u64,
                created_at: now,
                updated_at: now,
                content_type: payload.content_type,
                tags: payload.tags,
        checksum: None,
        storage_node: None,
    };

            match node.put_object(&payload.key, value, meta.clone()) {
                Ok(_) => {}
                Err(e) => return WsResponse::Error { message: e.to_string() },
            }

            WsResponse::Ok {
                data: serde_json::json!({
                    "meta": convert_meta(meta)
                }),
            }
        }

        WsRequest::Get(payload) => {
            let (value, meta) = match node.get_object(&payload.key) {
                Ok(Some(v)) => v,
                Ok(None) => return WsResponse::Error { message: "Key not found".to_string() },
                Err(e) => return WsResponse::Error { message: e.to_string() },
            };

            WsResponse::Ok {
                data: serde_json::json!({
                    "meta": meta.map(convert_meta),
                    "value": general_purpose::STANDARD.encode(value),
                }),
            }
        }

        WsRequest::Delete(payload) => {
            if let Err(e) = node.delete_object(&payload.key) {
                return WsResponse::Error { message: e.to_string() };
            }

            WsResponse::Ok {
                data: serde_json::json!({"success": true}),
            }
        }

        WsRequest::Exists(payload) => {
            let exists = match node.meta_exists(&payload.key) {
                Ok(e) => e,
                Err(e) => return WsResponse::Error { message: e.to_string() },
            };

            WsResponse::Ok {
                data: serde_json::json!({"exists": exists}),
            }
        }

        WsRequest::List(payload) => {
            let prefix = payload.prefix.unwrap_or_default();
            let limit = payload.limit.unwrap_or(100) as usize;

            let metas = match node.list_meta(&prefix, limit) {
                Ok(m) => m,
                Err(e) => return WsResponse::Error { message: e.to_string() },
            };

            let response_metas: Vec<ObjectMetaResponse> = metas.into_iter().map(convert_meta).collect();

            WsResponse::Ok {
                data: serde_json::json!({"metas": response_metas}),
            }
        }

        WsRequest::PutBatch(payload) => {
            let now = chrono::Utc::now();
            let mut items = Vec::with_capacity(payload.items.len());
            let mut metas = Vec::with_capacity(payload.items.len());

            for item in payload.items {
                let value = match general_purpose::STANDARD.decode(&item.value) {
                    Ok(v) => Bytes::from(v),
                    Err(e) => return WsResponse::Error { message: format!("Base64 解码失败: {}", e) },
                };

                let meta = crate::meta::ObjectMeta {
                    key: item.key.clone(),
                    size: value.len() as u64,
                    created_at: now,
                    updated_at: now,
                    content_type: item.content_type,
                    tags: item.tags,
        checksum: None,
        storage_node: None,
    };

                metas.push(meta.clone());
                items.push((item.key, value, meta));
            }

            if let Err(e) = node.put_objects_batch(items) {
                return WsResponse::Error { message: e.to_string() };
            }

            let response_metas: Vec<ObjectMetaResponse> = metas.into_iter().map(convert_meta).collect();

            WsResponse::Ok {
                data: serde_json::json!({"metas": response_metas}),
            }
        }
    }
}

/// 启动 Worker 的 WebSocket 服务
pub async fn start_worker_ws_server(node: Arc<WorkerNode>, port: u16) {
    let addr = format!("0.0.0.0:{}", port);

    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("[Worker WS] 绑定端口 {} 失败: {}", port, e);
            return;
        }
    };

    println!("🔌 Worker WebSocket server running on ws://{}", addr);

    while let Ok((stream, peer)) = listener.accept().await {
        let node = node.clone();
        tokio::spawn(async move {
            match accept_async(stream).await {
                Ok(ws_stream) => {
                    println!("[Worker WS] 新连接: {}", peer);
                    handle_connection(ws_stream, node).await;
                }
                Err(e) => {
                    eprintln!("[Worker WS] 接受连接失败: {}", e);
                }
            }
        });
    }
}
