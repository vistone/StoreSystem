use base64::{engine::general_purpose, Engine as _};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Instant;

#[allow(dead_code)]
use crate::grpc_client::key_to_quadkey;

#[derive(Debug, Serialize)]
#[allow(dead_code)]
struct BatchItemReq {
    key: String,
    value: String,
    content_type: Option<String>,
    tags: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
struct PutBatchReq {
    items: Vec<BatchItemReq>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
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

#[allow(dead_code)]
pub struct RestfulClient {
    base_url: String,
    http: Client,
}

#[allow(dead_code)]
impl RestfulClient {
    #[allow(dead_code)]
    pub fn new(base_url: &str, data_type: &str) -> Self {
        let base = format!("{}/{}", base_url.trim_end_matches('/'), data_type);
        Self {
            base_url: base,
            http: Client::new(),
        }
    }

    /// 单次写入测试，返回平均耗时（毫秒）
    /// 使用 quadkey 参数进行区域路由
    pub async fn put_bench(
        &self,
        rounds: usize,
        value_size: usize,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let value = vec![b'x'; value_size];
        let mut total_ms = 0.0;

        for i in 0..rounds {
            let key = format!("rest_put_{}", i);
            let quadkey = key_to_quadkey(&key);
            let url = format!(
                "{}/{}?content_type=text/plain&quadkey={}&level=10",
                self.base_url, key, quadkey
            );
            let start = Instant::now();
            let _resp: PutResp = self
                .http
                .post(&url)
                .body(value.clone())
                .send()
                .await?
                .json()
                .await?;
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }

        Ok(total_ms / rounds as f64)
    }

    /// 写入单条记录（用于测试前准备数据）
    pub async fn put_single(
        &self,
        key: &str,
        value: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let quadkey = key_to_quadkey(key);
        let url = format!(
            "{}/{}?content_type=text/plain&quadkey={}&level=10",
            self.base_url, key, quadkey
        );
        let _ = self.http.post(&url).body(value).send().await?;
        Ok(())
    }

    /// 读取单条记录
    pub async fn get_single(&self, key: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let quadkey = key_to_quadkey(key);
        let url = format!("{}/{}?quadkey={}&level=10", self.base_url, key, quadkey);
        let resp = self.http.get(&url).send().await?;
        if resp.status().is_success() {
            let r: GetResp = resp.json().await?;
            return Ok(general_purpose::STANDARD
                .decode(&r.value)
                .unwrap_or_default());
        }
        Ok(Vec::new())
    }

    /// 单次读取测试，返回 (平均耗时ms, 读取总耗时ms)
    #[allow(dead_code)]
    pub async fn get_bench(&self, rounds: usize) -> Result<(f64, f64), Box<dyn std::error::Error>> {
        // 先确保 key 存在
        let value = vec![b'x'; 1024];
        for i in 0..rounds {
            let key = format!("rest_get_{}", i);
            let quadkey = key_to_quadkey(&key);
            let url = format!(
                "{}/{}?content_type=text/plain&quadkey={}&level=10",
                self.base_url, key, quadkey
            );
            let _ = self.http.post(&url).body(value.clone()).send().await;
        }

        let mut total_ms = 0.0;
        for i in 0..rounds {
            let key = format!("rest_get_{}", i);
            let quadkey = key_to_quadkey(&key);
            let url = format!("{}/{}?quadkey={}&level=10", self.base_url, key, quadkey);
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
                .map(|i| {
                    let key = format!("rest_batch_{}_{}", r, i);
                    BatchItemReq {
                        key: key.clone(),
                        value: value_b64.clone(),
                        content_type: Some("text/plain".to_string()),
                        tags: None,
                    }
                })
                .collect();

            let url = format!("{}/batch", self.base_url);
            let req = PutBatchReq { items };
            let start = Instant::now();
            let _resp: serde_json::Value =
                self.http.post(&url).json(&req).send().await?.json().await?;
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
                    let key = format!("rest_conc_{}_{}", w, i);
                    let quadkey = key_to_quadkey(&key);
                    let url = format!(
                        "{}/{}?content_type=text/plain&quadkey={}&level=10",
                        base_url, key, quadkey
                    );
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

    // ============================================================
    // 高压测试方法
    // ============================================================

    /// 高压写入测试
    pub async fn stress_put(
        &self,
        total: usize,
        concurrency: usize,
        value_size: usize,
    ) -> (f64, f64, usize, usize) {
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
                let mut success = 0usize;
                let mut fail = 0usize;
                for i in 0..per_worker {
                    let key = format!("rest_stress_{}_{}", w, i);
                    let quadkey = key_to_quadkey(&key);
                    let url = format!(
                        "{}/{}?content_type=text/plain&quadkey={}&level=10",
                        base_url, key, quadkey
                    );
                    let s = Instant::now();
                    match http.post(&url).body(value.clone()).send().await {
                        Ok(_) => {
                            latencies.push(s.elapsed().as_secs_f64() * 1000.0);
                            success += 1;
                        }
                        Err(_) => {
                            fail += 1;
                        }
                    }
                }
                (latencies, success, fail)
            }));
        }

        let mut all_latencies = Vec::new();
        let mut total_success = 0usize;
        let mut total_fail = 0usize;
        for h in handles {
            if let Ok((lats, ok, fail)) = h.await {
                all_latencies.extend(lats);
                total_success += ok;
                total_fail += fail;
            }
        }

        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let avg_ms = if all_latencies.is_empty() {
            0.0
        } else {
            all_latencies.iter().sum::<f64>() / all_latencies.len() as f64
        };

        (avg_ms, total_ms, total_success, total_fail)
    }

    /// 长时间稳定性测试
    pub async fn stability_test(
        &self,
        duration_secs: u64,
        concurrency: usize,
        value_size: usize,
    ) -> (u64, u64, f64) {
        let value = vec![b'x'; value_size];
        let base_url = self.base_url.clone();
        let start = Instant::now();
        let end = start + std::time::Duration::from_secs(duration_secs);

        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let total_count = Arc::new(AtomicU64::new(0));
        let total_bytes = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();

        for w in 0..concurrency {
            let http = Client::new();
            let base_url = base_url.clone();
            let value = value.clone();

            let total_count = total_count.clone();
            let total_bytes = total_bytes.clone();
            handles.push(tokio::spawn(async move {
                let mut i = 0u64;
                while Instant::now() < end {
                    let key = format!("rest_stability_{}_{}", w, i);
                    let quadkey = key_to_quadkey(&key);
                    let url = format!(
                        "{}/{}?content_type=text/plain&quadkey={}&level=10",
                        base_url, key, quadkey
                    );
                    if http.post(&url).body(value.clone()).send().await.is_ok() {
                        total_count.fetch_add(1, Ordering::Relaxed);
                        total_bytes.fetch_add(value_size as u64, Ordering::Relaxed);
                    }
                    i += 1;
                }
            }));
        }

        // 每秒报告吞吐量
        let mut last_count = 0u64;
        let mut last_bytes = 0u64;
        let mut elapsed_sec = 0u64;
        while elapsed_sec < duration_secs {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            elapsed_sec += 1;
            let current_count = total_count.load(Ordering::Relaxed);
            let current_bytes = total_bytes.load(Ordering::Relaxed);
            let delta_count = current_count - last_count;
            let delta_bytes = current_bytes - last_bytes;
            let mbs = delta_bytes as f64 / 1024.0 / 1024.0;
            println!(
                "  [{}/{}s] {} ops/s, {:.1} MB/s (累计: {} 条, {:.1} MB)",
                elapsed_sec,
                duration_secs,
                delta_count,
                mbs,
                current_count,
                current_bytes as f64 / 1024.0 / 1024.0
            );
            last_count = current_count;
            last_bytes = current_bytes;
        }

        for h in handles {
            let _ = h.await;
        }

        let total_elapsed = start.elapsed().as_secs_f64();
        let final_count = total_count.load(Ordering::Relaxed);
        let final_bytes = total_bytes.load(Ordering::Relaxed);

        (final_count, final_bytes, total_elapsed)
    }
}
