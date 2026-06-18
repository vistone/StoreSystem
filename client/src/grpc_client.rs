use std::time::Instant;
use tonic::transport::Channel;
use base64::{Engine as _, engine::general_purpose};

pub mod proto {
    tonic::include_proto!("store");
}

use proto::store_service_client::StoreServiceClient;
use proto::{
    PutRequest, GetRequest, PutBatchRequest, BatchItem,
};

pub struct GrpcClient {
    client: StoreServiceClient<Channel>,
}

impl GrpcClient {
    pub async fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())?
            .timeout(std::time::Duration::from_secs(300))
            .connect_timeout(std::time::Duration::from_secs(5));
        let client = StoreServiceClient::connect(endpoint)
            .await?
            .max_decoding_message_size(256 * 1024 * 1024)
            .max_encoding_message_size(256 * 1024 * 1024);
        Ok(Self { client })
    }

    /// 单次写入测试，返回耗时（毫秒）
    pub async fn put_bench(&mut self, rounds: usize, value_size: usize) -> Result<f64, Box<dyn std::error::Error>> {
        let value = vec![b'x'; value_size];
        let mut total_ms = 0.0;

        for i in 0..rounds {
            let key = format!("grpc_put_{}", i);
            let req = tonic::Request::new(PutRequest {
                key: key.clone(),
                value: value.clone(),
                content_type: "text/plain".to_string(),
                tags: String::new(),
            });

            let start = Instant::now();
            let _resp = self.client.put(req).await?;
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }

        Ok(total_ms / rounds as f64)
    }

    /// 写入单条记录（用于测试前准备数据）
    pub async fn put_single(&mut self, key: &str, value: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let req = tonic::Request::new(PutRequest {
            key: key.to_string(),
            value,
            content_type: "text/plain".to_string(),
            tags: String::new(),
        });
        let _ = self.client.put(req).await?;
        Ok(())
    }

    /// 读取单条记录
    pub async fn get_single(&mut self, key: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let req = tonic::Request::new(GetRequest { key: key.to_string() });
        let resp = self.client.get(req).await?;
        Ok(resp.into_inner().value)
    }

    /// 单次读取测试，返回 (平均耗时ms, 读取总耗时ms)
    #[allow(dead_code)]
    pub async fn get_bench(&mut self, rounds: usize) -> Result<(f64, f64), Box<dyn std::error::Error>> {
        // 先确保 key 存在
        let value = vec![b'x'; 1024];
        for i in 0..rounds {
            let key = format!("grpc_get_{}", i);
            let req = tonic::Request::new(PutRequest {
                key: key.clone(),
                value: value.clone(),
                content_type: "text/plain".to_string(),
                tags: String::new(),
            });
            let _ = self.client.put(req).await;
        }

        let mut total_ms = 0.0;
        for i in 0..rounds {
            let key = format!("grpc_get_{}", i);
            let req = tonic::Request::new(GetRequest { key });

            let start = Instant::now();
            let _resp = self.client.get(req).await?;
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }

        Ok((total_ms / rounds as f64, total_ms))
    }

    /// 批量写入测试，返回平均耗时（毫秒）
    pub async fn put_batch_bench(
        &mut self,
        rounds: usize,
        batch_size: usize,
        value_size: usize,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let value = vec![b'x'; value_size];
        let mut total_ms = 0.0;

        for r in 0..rounds {
            let items: Vec<BatchItem> = (0..batch_size)
                .map(|i| BatchItem {
                    key: format!("grpc_batch_{}_{}", r, i),
                    value: value.clone(),
                    content_type: "text/plain".to_string(),
                    tags: String::new(),
                })
                .collect();

            let req = tonic::Request::new(PutBatchRequest { items });
            let start = Instant::now();
            let _resp = self.client.put_batch(req).await?;
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }

        Ok(total_ms / rounds as f64)
    }

    /// 并发写入测试，返回平均耗时（毫秒）和总耗时（毫秒）
    pub async fn put_concurrent_bench(
        &mut self,
        total: usize,
        concurrency: usize,
        value_size: usize,
    ) -> Result<(f64, f64), Box<dyn std::error::Error>> {
        let value = vec![b'x'; value_size];
        let client = self.client.clone();
        let start = Instant::now();

        let mut handles = Vec::new();
        let per_worker = total / concurrency;

        for w in 0..concurrency {
            let mut client = client.clone();
            let value = value.clone();
            handles.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(per_worker);
                for i in 0..per_worker {
                    let key = format!("grpc_conc_{}_{}", w, i);
                    let req = tonic::Request::new(PutRequest {
                        key,
                        value: value.clone(),
                        content_type: "text/plain".to_string(),
                        tags: String::new(),
                    });
                    let s = Instant::now();
                    let _ = client.put(req).await;
                    latencies.push(s.elapsed().as_secs_f64() * 1000.0);
                }
                latencies
            }));
        }

        let mut all_latencies = Vec::new();
        for h in handles {
            if let Ok(lats) = h.await {
                all_latencies.extend(lats);
            }
        }

        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let avg_ms = if all_latencies.is_empty() {
            0.0
        } else {
            all_latencies.iter().sum::<f64>() / all_latencies.len() as f64
        };

        Ok((avg_ms, total_ms))
    }
}

// 用于 RESTful 客户端共享 base64 编码
#[allow(dead_code)]
pub fn b64_encode(data: &[u8]) -> String {
    general_purpose::STANDARD.encode(data)
}
