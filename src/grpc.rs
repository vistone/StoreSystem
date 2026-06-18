use tonic::{Request, Response, Status};
use bytes::Bytes;
use crate::store::Store;

pub mod proto {
    tonic::include_proto!("store");
}

use proto::*;

#[derive(Debug, Clone)]
pub struct GrpcStoreService {
    store: Store,
}

impl GrpcStoreService {
    pub fn new(store: Store) -> Self {
        Self { store }
    }
    
    fn convert_meta(meta: crate::meta::ObjectMeta) -> ObjectMeta {
        ObjectMeta {
            key: meta.key,
            size: meta.size,
            created_at: meta.created_at.to_rfc3339(),
            updated_at: meta.updated_at.to_rfc3339(),
            content_type: meta.content_type.unwrap_or_default(),
            tags: meta.tags.map(|t| t.to_string()).unwrap_or_default(),
        }
    }
}

#[tonic::async_trait]
impl store_service_server::StoreService for GrpcStoreService {
    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        
        let content_type = if req.content_type.is_empty() { None } else { Some(req.content_type) };
        let tags = if req.tags.is_empty() {
            None
        } else {
            Some(serde_json::from_str(&req.tags)
                .map_err(|e| Status::invalid_argument(format!("Invalid tags JSON: {}", e)))?)
        };
        
        let meta = self.store.put(
            req.key.clone(),
            Bytes::from(req.value),
            content_type,
            tags
        ).await.map_err(|e| Status::internal(format!("Put failed for key '{}': {}", req.key, e)))?;
        
        Ok(Response::new(PutResponse {
            meta: Some(Self::convert_meta(meta))
        }))
    }
    
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        
        let (value, meta) = self.store.get(&req.key)
            .await
            .map_err(|e| match e {
                crate::error::StoreError::KeyNotFound(_) => Status::not_found(format!("Key not found: {}", req.key)),
                _ => Status::internal(format!("Get failed: {}", e))
            })?;
        
        Ok(Response::new(GetResponse {
            value: value.into(),
            meta: Some(Self::convert_meta(meta))
        }))
    }
    
    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        
        self.store.delete(&req.key)
            .await
            .map_err(|e| Status::internal(format!("Delete failed: {}", e)))?;
        
        Ok(Response::new(DeleteResponse { success: true }))
    }
    
    async fn exists(&self, request: Request<ExistsRequest>) -> Result<Response<ExistsResponse>, Status> {
        let req = request.into_inner();
        
        let exists = self.store.exists(&req.key)
            .await
            .map_err(|e| Status::internal(format!("Exists check failed: {}", e)))?;
        
        Ok(Response::new(ExistsResponse { exists }))
    }
    
    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let req = request.into_inner();
        
        let metas = self.store.list(&req.prefix, req.limit as usize)
            .await
            .map_err(|e| Status::internal(format!("List failed: {}", e)))?;
        
        let proto_metas = metas.into_iter().map(Self::convert_meta).collect();
        
        Ok(Response::new(ListResponse { metas: proto_metas }))
    }
    
    async fn put_batch(&self, request: Request<PutBatchRequest>) -> Result<Response<PutBatchResponse>, Status> {
        let req = request.into_inner();
        
        let mut items = Vec::with_capacity(req.items.len());
        for item in req.items {
            let content_type = if item.content_type.is_empty() { None } else { Some(item.content_type) };
            let tags = if item.tags.is_empty() {
                None
            } else {
                Some(serde_json::from_str(&item.tags)
                    .map_err(|e| Status::invalid_argument(format!("Invalid tags JSON: {}", e)))?)
            };
            
            items.push((
                item.key,
                Bytes::from(item.value),
                content_type,
                tags
            ));
        }
        
        let metas = self.store.put_batch(items)
            .await
            .map_err(|e| Status::internal(format!("Batch put failed: {}", e)))?;
        
        let proto_metas = metas.into_iter().map(Self::convert_meta).collect();
        
        Ok(Response::new(PutBatchResponse { metas: proto_metas }))
    }
}
