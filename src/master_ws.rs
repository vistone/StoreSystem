use crate::error::{Result, StoreError};
use crate::meta::ObjectMeta;
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// ============================================================
// WebSocket 消息协议（与 worker_ws.rs 保持一致）
// ============================================================

#[derive(Debug, Serialize)]
#[serde(tag = "action", content = "payload")]
enum WsRequest {
    Put(PutPayload),
    Get(GetPayload),
    Delete(DeletePayload),
    Exists(ExistsPayload),
    List(ListPayload),
    PutBatch(PutBatchPayload),
}

#[derive(Debug, Serialize)]
struct PutPayload {
    key: String,
    value: String,
    content_type: Option<String>,
    tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct GetPayload {
    key: String,
}

#[derive(Debug, Serialize)]
struct DeletePayload {
    key: String,
}

#[derive(Debug, Serialize)]
struct ExistsPayload {
    key: String,
}

#[derive(Debug, Serialize)]
struct ListPayload {
    prefix: String,
    limit: u32,
}

#[derive(Debug, Serialize)]
struct BatchItem {
    key: String,
    value: String,
    content_type: Option<String>,
    tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct PutBatchPayload {
    items: Vec<BatchItem>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status")]
enum WsResponse {
    #[serde(rename = "ok")]
    Ok { data: serde_json::Value },
    #[serde(rename = "error")]
    Error { message: String },
}

/// Master 通过 WebSocket 调用 Worker 的客户端
#[derive(Clone)]
pub struct WorkerWsClient {
    /// Worker 地址（不含 ws:// 前缀）
    address: String,
    /// 共享的 WebSocket 连接
    connection: Arc<Mutex<Option<WsConnection>>>,
}

impl std::fmt::Debug for WorkerWsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerWsClient")
            .field("address", &self.address)
            .finish()
    }
}

// WsConnection 不实现 Debug，因为 SplitSink/SplitStream 不实现 Debug
// 但 WorkerWsClient 需要 Debug，所以用 manual impl
struct WsConnection {
    writer: futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    reader: Arc<
        Mutex<
            futures_util::stream::SplitStream<
                tokio_tungstenite::WebSocketStream<
                    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
                >,
            >,
        >,
    >,
}

impl WorkerWsClient {
    pub fn new(address: &str) -> Self {
        // 标准化地址：去掉 ws:// 前缀
        let addr = address
            .trim_start_matches("ws://")
            .trim_start_matches("wss://");

        Self {
            address: addr.to_string(),
            connection: Arc::new(Mutex::new(None)),
        }
    }

    /// 获取或创建 WebSocket 连接
    async fn ensure_connected(&self) -> Result<()> {
        let mut guard = self.connection.lock().await;

        if guard.is_none() {
            let ws_url = format!("ws://{}/", self.address);
            let (ws_stream, _) = connect_async(&ws_url)
                .await
                .map_err(|e| StoreError::InvalidArgument(format!("WS 连接失败: {}", e)))?;

            let (writer, reader) = ws_stream.split();
            *guard = Some(WsConnection {
                writer,
                reader: Arc::new(Mutex::new(reader)),
            });
        }

        Ok(())
    }

    /// 发送请求并接收响应
    async fn send_request(&self, request: WsRequest) -> Result<serde_json::Value> {
        self.ensure_connected().await?;

        let mut guard = self.connection.lock().await;
        let conn = guard
            .as_mut()
            .ok_or_else(|| StoreError::InvalidArgument("WS 连接未初始化".to_string()))?;

        let request_text = serde_json::to_string(&request)
            .map_err(|e| StoreError::InvalidArgument(format!("请求序列化失败: {}", e)))?;

        // 发送请求
        conn.writer
            .send(Message::Text(request_text))
            .await
            .map_err(|e| StoreError::InvalidArgument(format!("WS 发送失败: {}", e)))?;

        // 接收响应
        let mut reader_guard = conn.reader.lock().await;
        loop {
            match reader_guard.next().await {
                Some(Ok(Message::Text(text))) => {
                    let response: WsResponse = serde_json::from_str(&text)
                        .map_err(|e| StoreError::InvalidArgument(format!("响应解析失败: {}", e)))?;

                    return match response {
                        WsResponse::Ok { data } => Ok(data),
                        WsResponse::Error { message } => Err(StoreError::InvalidArgument(message)),
                    };
                }
                Some(Ok(Message::Close(_))) => {
                    // 连接关闭，清除缓存
                    drop(reader_guard);
                    *guard = None;
                    return Err(StoreError::InvalidArgument("WS 连接已关闭".to_string()));
                }
                Some(Err(e)) => {
                    drop(reader_guard);
                    *guard = None;
                    return Err(StoreError::InvalidArgument(format!("WS 接收错误: {}", e)));
                }
                _ => continue,
            }
        }
    }

    fn convert_meta_response(data: &serde_json::Value) -> Result<ObjectMeta> {
        let key = data["key"]
            .as_str()
            .ok_or_else(|| StoreError::InvalidArgument("响应缺少 key 字段".to_string()))?
            .to_string();

        let created_at_str = data["created_at"]
            .as_str()
            .ok_or_else(|| StoreError::InvalidArgument("响应缺少 created_at 字段".to_string()))?;
        let created_at = chrono::DateTime::parse_from_rfc3339(created_at_str)
            .map_err(|e| StoreError::InvalidArgument(format!("created_at 时间解析失败: {}", e)))?
            .with_timezone(&chrono::Utc);

        let updated_at_str = data["updated_at"]
            .as_str()
            .ok_or_else(|| StoreError::InvalidArgument("响应缺少 updated_at 字段".to_string()))?;
        let updated_at = chrono::DateTime::parse_from_rfc3339(updated_at_str)
            .map_err(|e| StoreError::InvalidArgument(format!("updated_at 时间解析失败: {}", e)))?
            .with_timezone(&chrono::Utc);

        Ok(ObjectMeta {
            key,
            size: data["size"].as_u64().unwrap_or(0),
            created_at,
            updated_at,
            content_type: data["content_type"].as_str().map(|s| s.to_string()),
            tags: if data["tags"].is_null() {
                None
            } else {
                Some(data["tags"].clone())
            },
            checksum: None,
            storage_node: None,
        })
    }

    pub async fn put(
        &self,
        key: &str,
        value: Bytes,
        content_type: Option<&str>,
        tags: Option<&str>,
    ) -> Result<ObjectMeta> {
        let tags_value: Option<serde_json::Value> = tags.and_then(|t| serde_json::from_str(t).ok());

        let request = WsRequest::Put(PutPayload {
            key: key.to_string(),
            value: general_purpose::STANDARD.encode(value),
            content_type: content_type.map(|s| s.to_string()),
            tags: tags_value,
        });

        let data = self.send_request(request).await?;
        let meta_data = &data["meta"];
        Self::convert_meta_response(meta_data)
    }

    pub async fn get(&self, key: &str) -> Result<(Bytes, ObjectMeta)> {
        let request = WsRequest::Get(GetPayload {
            key: key.to_string(),
        });

        let data = self.send_request(request).await?;

        let value =
            general_purpose::STANDARD
                .decode(data["value"].as_str().ok_or_else(|| {
                    StoreError::InvalidArgument("响应缺少 value 字段".to_string())
                })?)
                .map_err(|e| StoreError::InvalidArgument(format!("Base64 解码失败: {}", e)))?;

        let meta = Self::convert_meta_response(&data["meta"])?;
        Ok((Bytes::from(value), meta))
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let request = WsRequest::Delete(DeletePayload {
            key: key.to_string(),
        });

        self.send_request(request).await?;
        Ok(())
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        let request = WsRequest::Exists(ExistsPayload {
            key: key.to_string(),
        });

        let data = self.send_request(request).await?;
        Ok(data["exists"].as_bool().unwrap_or(false))
    }

    pub async fn list(&self, prefix: &str, limit: u32) -> Result<Vec<ObjectMeta>> {
        let request = WsRequest::List(ListPayload {
            prefix: prefix.to_string(),
            limit,
        });

        let data = self.send_request(request).await?;
        let metas_data = data["metas"]
            .as_array()
            .ok_or_else(|| StoreError::InvalidArgument("无效的 metas 响应".to_string()))?;

        let mut metas = Vec::with_capacity(metas_data.len());
        for m in metas_data {
            metas.push(Self::convert_meta_response(m)?);
        }

        Ok(metas)
    }

    pub async fn put_batch(
        &self,
        items: Vec<(String, Bytes, Option<String>, Option<serde_json::Value>)>,
    ) -> Result<Vec<ObjectMeta>> {
        let batch_items: Vec<BatchItem> = items
            .into_iter()
            .map(|(key, value, content_type, tags)| BatchItem {
                key,
                value: general_purpose::STANDARD.encode(value),
                content_type,
                tags,
            })
            .collect();

        let request = WsRequest::PutBatch(PutBatchPayload { items: batch_items });

        let data = self.send_request(request).await?;
        let metas_data = data["metas"]
            .as_array()
            .ok_or_else(|| StoreError::InvalidArgument("无效的 metas 响应".to_string()))?;

        let mut metas = Vec::with_capacity(metas_data.len());
        for m in metas_data {
            metas.push(Self::convert_meta_response(m)?);
        }

        Ok(metas)
    }
}
