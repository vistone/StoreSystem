use std::time::Instant;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use base64::{Engine as _, engine::general_purpose};

#[derive(Debug, Serialize)]
struct BatchItemReq {
    key: String,
    value: String,
    content_type: Option<String>,
    tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct PutBatchReq {
    items: Vec<BatchItemReq>,
}

#[derive(Debug, Deserialize)]
struct PutResp {
    #[allow(dead_code)]
    meta: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct GetResp {
    #[allow(dead_code)]
    meta: serde_json::Value,
    #[allow(dead_code)]
    value: String,
}

pub struct RestfulClient {
    base_url: String,
    http: Client,
}

impl RestfulClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: Client::new(),
        }
    }

    /// 单次写入测试，返回平均耗时（毫秒）
    pub async fn put_bench(&self, rounds: usize, value_size: usize) -> Result<f64, Box<dyn std::error::Error>> {
        let value = vec![b'x'; value_size];
        let mut total_ms = 0.0;

        for i in 0..rounds {
            let url = format!("{}/objects/rest_put_{}?content_type=text/plain", self.base_url, i);
            let start = Instant::now();
            let _resp: PutResp = self.http.post(&url).body(value.clone()).send().await?.json().await?;
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }

        Ok(total_ms / rounds as f64)
    }

    /// 写入单条记录（用于测试前准备数据）
    pub async fn put_single(&self, key: &str, value: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let url = format!("{}/objects/{}?content_type=text/plain", self.base_url, key);
        let _ = self.http.post(&url).body(value).send().await?;
        Ok(())
    }

    /// 读取单条记录
    pub async fn get_single(&self, key: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let url = format!("{}/objects/{}", self.base_url, key);
        let resp = self.http.get(&url).send().await?;
        if resp.status().is_success() {
            let r: GetResp = resp.json().await?;
            return Ok(general_purpose::STANDARD.decode(&r.value).unwrap_or_default());
        }
        Ok(Vec::new())
    }

    /// 单次读取测试，返回 (平均耗时ms, 读取总耗时ms)
    #[allow(dead_code)]
    pub async fn get_bench(&self, rounds: usize) -> Result<(f64, f64), Box<dyn std::error::Error>> {
        // 先确保 key 存在
        let value = vec![b'x'; 1024];
        for i in 0..rounds {
            let url = format!("{}/objects/rest_get_{}?content_type=text/plain", self.base_url, i);
            let _ = self.http.post(&url).body(value.clone()).send().await;
        }

        let mut total_ms = 0.0;
        for i in 0..rounds {
            let url = format!("{}/objects/rest_get_{}", self.base_url, i);
            let start = Instant::now();
            let resp = self.http.get(&url).send().await?;
            if resp.status().is_success() {
                let _resp: GetResp = resp.json().await?;
            }
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }

        Ok((total_ms / rounds as f64, total_ms))
    }

    /// 批量写入测试，返回平均耗时（毫秒）
    pub async fn put_batch_bench(
        &self,
        rounds: usize,
        batch_size: usize,
        value_size: usize,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let value_b64 = general_purpose::STANDARD.encode(vec![b'x'; value_size]);
        let mut total_ms = 0.0;

        for r in 0..rounds {
            let items: Vec<BatchItemReq> = (0..batch_size)
                .map(|i| BatchItemReq {
                    key: format!("rest_batch_{}_{}", r, i),
                    value: value_b64.clone(),
                    content_type: Some("text/plain".to_string()),
                    tags: None,
                })
                .collect();

            let url = format!("{}/objects/batch", self.base_url);
            let req = PutBatchReq { items };
            let start = Instant::now();
            let _resp: serde_json::Value = self.http.post(&url).json(&req).send().await?.json().await?;
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }

        Ok(total_ms / rounds as f64)
    }

    /// 并发写入测试，返回平均耗时（毫秒）和总耗时（毫秒）
    pub async fn put_concurrent_bench(
        &self,
        total: usize,
        concurrency: usize,
        value_size: usize,
    ) -> Result<(f64, f64), Box<dyn std::error::Error>> {
        let value = vec![b'x'; value_size];
        let base_url = self.base_url.clone();
        let start = Instant::now();

        let mut handles = Vec::new();
        let per_worker = total / concurrency;

        for w in 0..concurrency {
            let http = Client::new();
            let base_url = base_url.clone();
            let value = value.clone();
            handles.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(per_worker);
                for i in 0..per_worker {
                    let url = format!("{}/objects/rest_conc_{}_{}?content_type=text/plain", base_url, w, i);
                    let s = Instant::now();
                    let _ = http.post(&url).body(value.clone()).send().await;
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
