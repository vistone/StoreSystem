use crate::error::StoreError;
use crate::store::Store;
use base64::{engine::general_purpose, Engine as _};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use warp::{http::StatusCode, reply::Json, reply::WithStatus, Filter, Rejection, Reply};

#[derive(Debug)]
struct CustomReject(StatusCode, String);

impl warp::reject::Reject for CustomReject {}

/// PUT 请求查询参数
#[derive(Debug, Deserialize)]
pub struct PutQuery {
    pub content_type: Option<String>,
    pub tags: Option<String>,
    pub quadkey: Option<String>,
    pub level: Option<u32>,
}

/// 列表查询参数
#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub prefix: Option<String>,
    pub limit: Option<u32>,
}

/// 对象元数据响应
#[derive(Debug, Serialize)]
pub struct ObjectMetaResponse {
    pub key: String,
    pub size: u64,
    pub created_at: String,
    pub updated_at: String,
    pub content_type: Option<String>,
    pub tags: Option<serde_json::Value>,
}

/// 写入响应
#[derive(Debug, Serialize)]
pub struct PutResponse {
    pub meta: ObjectMetaResponse,
}

/// 读取响应（value 为 base64 编码）
#[derive(Debug, Serialize)]
pub struct GetResponse {
    pub meta: ObjectMetaResponse,
    pub value: String, // base64 编码的二进制数据
}

/// 删除响应
#[derive(Debug, Serialize)]
pub struct DeleteResponse {
    pub success: bool,
}

/// 存在检查响应
#[derive(Debug, Serialize)]
pub struct ExistsResponse {
    pub exists: bool,
}

/// 列表响应
#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub metas: Vec<ObjectMetaResponse>,
}

/// 批量写入请求中的单条
#[derive(Debug, Deserialize)]
pub struct BatchItem {
    pub key: String,
    pub value: String, // base64
    pub content_type: Option<String>,
    pub tags: Option<serde_json::Value>,
}

/// 批量写入请求体
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
    store: Arc<Store>,
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

    let meta = store
        .put(key, body, query.content_type, tags)
        .await
        .map_err(|e| match e {
            StoreError::InvalidArgument(_) => warp::reject::custom(CustomReject(
                StatusCode::BAD_REQUEST,
                "Invalid argument".to_string(),
            )),
            _ => warp::reject::custom(CustomReject(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal server error".to_string(),
            )),
        })?;

    Ok(warp::reply::json(&PutResponse {
        meta: convert_meta(meta),
    }))
}

async fn get_handler(
    _data_type: String,
    key: String,
    store: Arc<Store>,
) -> Result<impl Reply, Rejection> {
    let (value, meta) = store.get(&key).await.map_err(|e| match e {
        StoreError::KeyNotFound(_) => warp::reject::custom(CustomReject(
            StatusCode::NOT_FOUND,
            "Key not found".to_string(),
        )),
        _ => warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
        )),
    })?;

    Ok(warp::reply::json(&GetResponse {
        meta: convert_meta(meta),
        value: general_purpose::STANDARD.encode(value),
    }))
}

async fn delete_handler(
    _data_type: String,
    key: String,
    store: Arc<Store>,
) -> Result<impl Reply, Rejection> {
    store.delete(&key).await.map_err(|_| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
        ))
    })?;

    Ok(warp::reply::json(&DeleteResponse { success: true }))
}

async fn exists_handler(
    _data_type: String,
    key: String,
    store: Arc<Store>,
) -> Result<impl Reply, Rejection> {
    let exists = store.exists(&key).await.map_err(|_| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
        ))
    })?;

    Ok(warp::reply::json(&ExistsResponse { exists }))
}

async fn list_handler(
    _data_type: String,
    query: ListQuery,
    store: Arc<Store>,
) -> Result<impl Reply, Rejection> {
    let prefix = query.prefix.unwrap_or_default();
    let limit = query.limit.unwrap_or(100) as usize;

    let metas = store.list(&prefix, limit).await.map_err(|_| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
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
    store: Arc<Store>,
) -> Result<impl Reply, Rejection> {
    let mut items = Vec::with_capacity(req.items.len());
    for item in req.items {
        let value = general_purpose::STANDARD.decode(&item.value).map_err(|_| {
            warp::reject::custom(CustomReject(
                StatusCode::BAD_REQUEST,
                "Invalid base64 value".to_string(),
            ))
        })?;

        items.push((item.key, Bytes::from(value), item.content_type, item.tags));
    }

    let metas = store.put_batch(items).await.map_err(|_| {
        warp::reject::custom(CustomReject(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal server error".to_string(),
        ))
    })?;

    let response_metas = metas.into_iter().map(convert_meta).collect();

    Ok(warp::reply::json(&ListResponse {
        metas: response_metas,
    }))
}

/// CORS 配置：允许跨域请求
fn cors() -> warp::cors::Cors {
    warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "DELETE", "PUT", "OPTIONS"])
        .allow_headers(vec!["Content-Type", "Authorization", "X-Requested-With"])
        .build()
}

/// 启动 RESTful API 服务（warp）
pub async fn start_server(store: Store, port: u16) {
    let store = Arc::new(store);

    // POST /:data_type/:key  写入对象
    let store_put = store.clone();
    let put_route = warp::path!(String / String)
        .and(warp::post())
        .and(warp::query::<PutQuery>())
        .and(warp::body::bytes())
        .and(warp::any().map(move || store_put.clone()))
        .and_then(put_handler);

    // GET /:data_type/:key  读取对象
    let store_get = store.clone();
    let get_route = warp::path!(String / String)
        .and(warp::get())
        .and(warp::any().map(move || store_get.clone()))
        .and_then(get_handler);

    // DELETE /:data_type/:key  删除对象
    let store_del = store.clone();
    let delete_route = warp::path!(String / String)
        .and(warp::delete())
        .and(warp::any().map(move || store_del.clone()))
        .and_then(delete_handler);

    // GET /:data_type/:key/exists  检查对象是否存在
    let store_exists = store.clone();
    let exists_route = warp::path!(String / String / "exists")
        .and(warp::get())
        .and(warp::any().map(move || store_exists.clone()))
        .and_then(exists_handler);

    // GET /:data_type?prefix=xxx&limit=100  按前缀列出对象
    let store_list = store.clone();
    let list_route = warp::path!(String)
        .and(warp::path::end())
        .and(warp::get())
        .and(warp::query::<ListQuery>())
        .and(warp::any().map(move || store_list.clone()))
        .and_then(list_handler);

    // POST /:data_type/batch  批量写入对象
    let store_batch = store.clone();
    let batch_route = warp::path!(String / "batch")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::any().map(move || store_batch.clone()))
        .and_then(put_batch_handler);

    let routes = batch_route
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

    println!("🌐 RESTful API server running on http://0.0.0.0:{}", port);
    warp::serve(routes).bind(([0, 0, 0, 0], port)).await;
}
