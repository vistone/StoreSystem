use crate::worker::WorkerNode;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use warp::{http::StatusCode, reply::Json, reply::WithStatus, Filter, Rejection, Reply};

use base64::{engine::general_purpose, Engine as _};

#[derive(Debug)]
struct CustomReject(StatusCode, String);

impl warp::reject::Reject for CustomReject {}

#[derive(Debug, Deserialize)]
pub struct PutQuery {
    pub content_type: Option<String>,
    pub tags: Option<String>,
    pub quadkey: Option<String>,
    pub level: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub prefix: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct ObjectMetaResponse {
    pub key: String,
    pub size: u64,
    pub created_at: String,
    pub updated_at: String,
    pub content_type: Option<String>,
    pub tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct PutResponse {
    pub meta: ObjectMetaResponse,
}

#[derive(Debug, Serialize)]
pub struct GetResponse {
    pub meta: Option<ObjectMetaResponse>,
    pub value: String, // base64 编码
}

#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub success: bool,
}

#[derive(Debug, Serialize)]
pub struct ExistsResponse {
    pub exists: bool,
}

#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub metas: Vec<ObjectMetaResponse>,
}

#[derive(Debug, Deserialize)]
pub struct BatchItem {
    pub key: String,
    pub value: String, // base64
    pub content_type: Option<String>,
    pub tags: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct PutBatchRequest {
    pub items: Vec<BatchItem>,
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

async fn put_handler(
    _data_type: String,
    key: String,
    query: PutQuery,
    body: Bytes,
    node: Arc<WorkerNode>,
) -> Result<impl Reply, Rejection> {
    let tags = if let Some(tags_str) = query.tags {
        Some(serde_json::from_str(&tags_str).map_err(|_| {
            warp::reject::custom(CustomReject(
                StatusCode::BAD_REQUEST,
                "Invalid tags JSON".to_string(),
            ))
        })?)
    } else {
        None
    };

    let now = chrono::Utc::now();
    let meta = crate::meta::ObjectMeta {
        key: key.clone(),
        size: body.len() as u64,
        created_at: now,
        updated_at: now,
        content_type: query.content_type,
        tags,
        checksum: None,
        storage_node: None,
    };

    node.put_object(&key, body, meta.clone()).map_err(|e| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string(),
        ))
    })?;

    Ok(warp::reply::json(&PutResponse {
        meta: convert_meta(meta),
    }))
}

async fn get_handler(
    _data_type: String,
    key: String,
    node: Arc<WorkerNode>,
) -> Result<impl Reply, Rejection> {
    let (value, meta) = node
        .get_object(&key)
        .map_err(|e| {
            warp::reject::custom(CustomReject(
                StatusCode::INTERNAL_SERVER_ERROR,
                e.to_string(),
            ))
        })?
        .ok_or_else(|| {
            warp::reject::custom(CustomReject(
                StatusCode::NOT_FOUND,
                "Key not found".to_string(),
            ))
        })?;

    Ok(warp::reply::json(&GetResponse {
        meta: meta.map(convert_meta),
        value: general_purpose::STANDARD.encode(value),
    }))
}

async fn delete_handler(
    _data_type: String,
    key: String,
    node: Arc<WorkerNode>,
) -> Result<impl Reply, Rejection> {
    node.delete_object(&key).map_err(|e| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string(),
        ))
    })?;

    Ok(warp::reply::json(&DeleteResponse { success: true }))
}

async fn exists_handler(
    _data_type: String,
    key: String,
    node: Arc<WorkerNode>,
) -> Result<impl Reply, Rejection> {
    let exists = node.meta_exists(&key).map_err(|e| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string(),
        ))
    })?;

    Ok(warp::reply::json(&ExistsResponse { exists }))
}

async fn list_handler(
    _data_type: String,
    query: ListQuery,
    node: Arc<WorkerNode>,
) -> Result<impl Reply, Rejection> {
    let prefix = query.prefix.unwrap_or_default();
    let limit = query.limit.unwrap_or(100) as usize;

    let metas = node.list_meta(&prefix, limit).map_err(|e| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string(),
        ))
    })?;

    let response_metas = metas.into_iter().map(convert_meta).collect();
    Ok(warp::reply::json(&ListResponse {
        metas: response_metas,
    }))
}

async fn put_batch_handler(
    _data_type: String,
    req: PutBatchRequest,
    node: Arc<WorkerNode>,
) -> Result<impl Reply, Rejection> {
    let now = chrono::Utc::now();
    let mut items = Vec::with_capacity(req.items.len());
    let mut metas = Vec::with_capacity(req.items.len());

    for item in req.items {
        let value = general_purpose::STANDARD.decode(&item.value).map_err(|_| {
            warp::reject::custom(CustomReject(
                StatusCode::BAD_REQUEST,
                "Invalid base64 value".to_string(),
            ))
        })?;
        let value = Bytes::from(value);

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

    node.put_objects_batch(items).map_err(|e| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            e.to_string(),
        ))
    })?;

    let response_metas = metas.into_iter().map(convert_meta).collect();
    Ok(warp::reply::json(&ListResponse {
        metas: response_metas,
    }))
}

/// CORS 配置
fn cors() -> warp::cors::Cors {
    warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "DELETE", "PUT", "OPTIONS"])
        .allow_headers(vec!["Content-Type", "Authorization", "X-Requested-With"])
        .build()
}

/// 启动 Worker 的 RESTful HTTP 服务
pub async fn start_worker_http_server(node: Arc<WorkerNode>, port: u16) {
    // POST /:data_type/:key  写入对象
    let node_put = node.clone();
    let put_route = warp::path!(String / String)
        .and(warp::post())
        .and(warp::query::<PutQuery>())
        .and(warp::body::bytes())
        .and(warp::any().map(move || node_put.clone()))
        .and_then(put_handler);

    // GET /:data_type/:key  读取对象
    let node_get = node.clone();
    let get_route = warp::path!(String / String)
        .and(warp::get())
        .and(warp::any().map(move || node_get.clone()))
        .and_then(get_handler);

    // DELETE /:data_type/:key  删除对象
    let node_del = node.clone();
    let delete_route = warp::path!(String / String)
        .and(warp::delete())
        .and(warp::any().map(move || node_del.clone()))
        .and_then(delete_handler);

    // GET /:data_type/:key/exists  检查对象是否存在
    let node_exists = node.clone();
    let exists_route = warp::path!(String / String / "exists")
        .and(warp::get())
        .and(warp::any().map(move || node_exists.clone()))
        .and_then(exists_handler);

    // GET /:data_type?prefix=xxx&limit=100  按前缀列出对象
    let node_list = node.clone();
    let list_route = warp::path!(String)
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query::<ListQuery>())
        .and(warp::any().map(move || node_list.clone()))
        .and_then(list_handler);

    // POST /:data_type/batch  批量写入对象
    let node_batch = node.clone();
    let batch_route = warp::path!(String / "batch")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::any().map(move || node_batch.clone()))
        .and_then(put_batch_handler);

    // GET /health  健康检查
    let health_route = warp::path("health")
        .and(warp::path::end())
        .and(warp::get())
        .map(|| {
            warp::reply::json(&serde_json::json!({
                "status": "ok",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }))
        });

    let routes = health_route
        .or(batch_route)
        .or(exists_route)
        .or(list_route)
        .or(put_route)
        .or(get_route)
        .or(delete_route)
        .with(cors())
        .recover(|err: Rejection| async move {
            let result: Result<WithStatus<Json>, Rejection> =
                if let Some(custom) = err.find::<CustomReject>() {
                    Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({"error": custom.1})),
                        custom.0,
                    ))
                } else {
                    Ok(warp::reply::with_status(
                        warp::reply::json(&serde_json::json!({"error": "Internal Server Error"})),
                        StatusCode::INTERNAL_SERVER_ERROR,
                    ))
                };
            result
        });

    println!(
        "🌐 Worker RESTful API server running on http://0.0.0.0:{}",
        port
    );
    warp::serve(routes).bind(([0, 0, 0, 0], port)).await;
}
