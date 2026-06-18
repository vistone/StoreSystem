use bytes::Bytes;
use serde::{Deserialize, Serialize};
use crate::error::{Result, StoreError};
use crate::meta::ObjectMeta;
use base64::{Engine as _, engine::general_purpose};

/// Master 通过 RESTful API 调用 Worker 的 HTTP 客户端
#[derive(Debug, Clone)]
pub struct WorkerHttpClient {


    base_url: String,
    client: reqwest::Client,
}

#[derive(Debug, Deserialize)]
struct PutResponse {
    meta: ObjectMetaResponse,
}

#[derive(Debug, Deserialize)]
struct GetResponse {
    meta: ObjectMetaResponse,
    value: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct DeleteResponse {
    success: bool,
}

#[derive(Debug, Deserialize)]
struct ExistsResponse {
    exists: bool,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    metas: Vec<ObjectMetaResponse>,
}

#[derive(Debug, Deserialize)]
struct ObjectMetaResponse {
    key: String,
    size: u64,
    created_at: String,
    updated_at: String,
    content_type: Option<String>,
    tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct BatchItem {
    key: String,
    value: String,
    content_type: Option<String>,
    tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct PutBatchRequest {
    items: Vec<BatchItem>,
}

impl WorkerHttpClient {
    pub fn new(address: &str) -> Self {
        let base_url = if address.starts_with("http") {
            format!("{}/objects", address)
        } else {
            format!("http://{}/objects", address)
        };

        Self {
            base_url,
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .connect_timeout(std::time::Duration::from_secs(5))
                .build()
                .expect("Failed to create HTTP client"),
        }
    }

    fn convert_meta_response(resp: ObjectMetaResponse) -> Result<ObjectMeta> {
        Ok(ObjectMeta {
            key: resp.key,
            size: resp.size,
            created_at: chrono::DateTime::parse_from_rfc3339(&resp.created_at)
                .map_err(|e| StoreError::InvalidArgument(format!("时间解析失败: {}", e)))?
                .with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(&resp.updated_at)
                .map_err(|e| StoreError::InvalidArgument(format!("时间解析失败: {}", e)))?
                .with_timezone(&chrono::Utc),
            content_type: resp.content_type,
            tags: resp.tags,
            checksum: None,
            storage_node: None,
        })
    }


    pub async fn put(&self, key: &str, value: Bytes, content_type: Option<&str>, tags: Option<&str>) -> Result<ObjectMeta> {
        let url = format!("{}/{}", self.base_url, key);
        let mut req = self.client.post(&url).body(value.to_vec());

        if let Some(ct) = content_type {
            req = req.query(&[("content_type", ct)]);
        }
        if let Some(t) = tags {
            req = req.query(&[("tags", t)]);
        }

        let resp = req.send().await
            .map_err(|e| StoreError::InvalidArgument(format!("HTTP 请求失败: {}", e)))?;

        if !resp.status().is_success() {
            let err_text = resp.text().await.unwrap_or_default();
            return Err(StoreError::InvalidArgument(format!("Worker HTTP 写入失败: {}", err_text)));
        }

        let put_resp: PutResponse = resp.json().await
            .map_err(|e| StoreError::InvalidArgument(format!("响应解析失败: {}", e)))?;

        Self::convert_meta_response(put_resp.meta)
    }

    pub async fn get(&self, key: &str) -> Result<(Bytes, ObjectMeta)> {
        let url = format!("{}/{}", self.base_url, key);

        let resp = self.client.get(&url).send().await
            .map_err(|e| StoreError::InvalidArgument(format!("HTTP 请求失败: {}", e)))?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(StoreError::MetaNotFound(key.to_string()));
        }
        if !resp.status().is_success() {
            let err_text = resp.text().await.unwrap_or_default();
            return Err(StoreError::InvalidArgument(format!("Worker HTTP 读取失败: {}", err_text)));
        }

        let get_resp: GetResponse = resp.json().await
            .map_err(|e| StoreError::InvalidArgument(format!("响应解析失败: {}", e)))?;

        let value = general_purpose::STANDARD.decode(&get_resp.value)
            .map_err(|e| StoreError::InvalidArgument(format!("Base64 解码失败: {}", e)))?;

        let meta = Self::convert_meta_response(get_resp.meta)?;
        Ok((Bytes::from(value), meta))
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let url = format!("{}/{}", self.base_url, key);

        let resp = self.client.delete(&url).send().await
            .map_err(|e| StoreError::InvalidArgument(format!("HTTP 请求失败: {}", e)))?;

        if !resp.status().is_success() {
            let err_text = resp.text().await.unwrap_or_default();
            return Err(StoreError::InvalidArgument(format!("Worker HTTP 删除失败: {}", err_text)));
        }

        Ok(())
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        let url = format!("{}/{}/exists", self.base_url, key);

        let resp = self.client.get(&url).send().await
            .map_err(|e| StoreError::InvalidArgument(format!("HTTP 请求失败: {}", e)))?;

        if !resp.status().is_success() {
            let err_text = resp.text().await.unwrap_or_default();
            return Err(StoreError::InvalidArgument(format!("Worker HTTP 检查失败: {}", err_text)));
        }

        let exists_resp: ExistsResponse = resp.json().await
            .map_err(|e| StoreError::InvalidArgument(format!("响应解析失败: {}", e)))?;

        Ok(exists_resp.exists)
    }

    pub async fn list(&self, prefix: &str, limit: u32) -> Result<Vec<ObjectMeta>> {
        let resp = self.client.get(&self.base_url)
            .query(&[("prefix", prefix), ("limit", &limit.to_string())])
            .send().await
            .map_err(|e| StoreError::InvalidArgument(format!("HTTP 请求失败: {}", e)))?;

        if !resp.status().is_success() {
            let err_text = resp.text().await.unwrap_or_default();
            return Err(StoreError::InvalidArgument(format!("Worker HTTP 列出失败: {}", err_text)));
        }

        let list_resp: ListResponse = resp.json().await
            .map_err(|e| StoreError::InvalidArgument(format!("响应解析失败: {}", e)))?;

        let mut metas = Vec::with_capacity(list_resp.metas.len());
        for m in list_resp.metas {
            metas.push(Self::convert_meta_response(m)?);
        }

        Ok(metas)
    }

    pub async fn put_batch(&self, items: Vec<(String, Bytes, Option<String>, Option<serde_json::Value>)>) -> Result<Vec<ObjectMeta>> {
        let url = format!("{}/batch", self.base_url);

        let batch_items: Vec<BatchItem> = items.into_iter().map(|(key, value, content_type, tags)| {
            BatchItem {
                key,
                value: general_purpose::STANDARD.encode(value),
                content_type,
                tags,
            }
        }).collect();

        let req = PutBatchRequest { items: batch_items };

        let resp = self.client.post(&url)
            .json(&req)
            .send().await
            .map_err(|e| StoreError::InvalidArgument(format!("HTTP 请求失败: {}", e)))?;

        if !resp.status().is_success() {
            let err_text = resp.text().await.unwrap_or_default();
            return Err(StoreError::InvalidArgument(format!("Worker HTTP 批量写入失败: {}", err_text)));
        }

        let list_resp: ListResponse = resp.json().await
            .map_err(|e| StoreError::InvalidArgument(format!("响应解析失败: {}", e)))?;

        let mut metas = Vec::with_capacity(list_resp.metas.len());
        for m in list_resp.metas {
            metas.push(Self::convert_meta_response(m)?);
        }

        Ok(metas)
    }
}
