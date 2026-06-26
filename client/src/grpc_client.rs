use std::hash::{Hash, Hasher};
use std::time::Instant;
use tonic::transport::Channel;

pub mod proto {
    tonic::include_proto!("store");
}

use proto::store_service_client::StoreServiceClient;
use proto::{GetRequest, PutRequest};

#[allow(dead_code)]
pub fn key_to_quadkey(key: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    let h = hasher.finish();
    let region = h % 4;
    format!("{:04}", region)
}

pub struct GrpcClient {
    #[allow(dead_code)]
    client: StoreServiceClient<Channel>,
}

#[allow(dead_code)]
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

    // ============================================================
    // 基础操作
    // ============================================================

    /// 写入单条记录（带 quadkey 路由）
    pub async fn put(
        &mut self,
        key: &str,
        value: Vec<u8>,
        quadkey: &str,
        level: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let req = tonic::Request::new(PutRequest {
            key: key.to_string(),
            value,
            content_type: "text/plain".to_string(),
            tags: String::new(),
            quadkey: quadkey.to_string(),
            level,
            ..Default::default()
        });
        let _ = self.client.put(req).await?;
        Ok(())
    }

    /// 读取单条记录
    pub async fn get(
        &mut self,
        key: &str,
        quadkey: &str,
        level: u32,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let req = tonic::Request::new(GetRequest {
            key: key.to_string(),
            quadkey: quadkey.to_string(),
            level,
            ..Default::default()
        });
        let resp = self.client.get(req).await?;
        Ok(resp.into_inner().value)
    }

    // ============================================================
    // 全新高压测试方法
    // ============================================================

    /// 测试 1: 4 区域路由验证
    /// 向 region 0/1/2/3 各写 count 条，然后全部读取验证
    pub async fn quadkey_routing_stress_test(
        &mut self,
        count: usize,
    ) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let regions = ["0000", "1111", "2222", "3333"];
        let region_names = ["0", "1", "2", "3"];
        let mut success = 0usize;
        let mut fail = 0usize;

        for (ridx, region) in regions.iter().enumerate() {
            let rname = region_names[ridx];
            for i in 0..count {
                let key = format!("qrst_{}_{}", rname, i);
                let value = format!("val_{}_{}", rname, i).into_bytes();
                let req = tonic::Request::new(PutRequest {
                    key: key.clone(),
                    value,
                    content_type: "text/plain".to_string(),
                    tags: String::new(),
                    quadkey: region.to_string(),
                    level: 10,
                    ..Default::default()
                });
                match self.client.put(req).await {
                    Ok(_) => success += 1,
                    Err(e) => {
                        println!("    ✗ 写入失败 region={} key={}: {}", region, key, e);
                        fail += 1;
                    }
                }
            }
        }

        for (ridx, region) in regions.iter().enumerate() {
            let rname = region_names[ridx];
            for i in 0..count {
                let key = format!("qrst_{}_{}", rname, i);
                let expected = format!("val_{}_{}", rname, i).into_bytes();
                let req = tonic::Request::new(GetRequest {
                    key: key.clone(),
                    quadkey: region.to_string(),
                    level: 10,
                    ..Default::default()
                });
                match self.client.get(req).await {
                    Ok(resp) => {
                        if resp.into_inner().value == expected {
                            success += 1;
                        } else {
                            println!("    ✗ 值不匹配 region={} key={}", rname, key);
                            fail += 1;
                        }
                    }
                    Err(e) => {
                        println!("    ✗ 读取失败 region={} key={}: {}", rname, key, e);
                        fail += 1;
                    }
                }
            }
        }

        Ok((success, fail))
    }

    /// 测试 2: 3 级分片验证
    /// level=5(→base), level=12(→4位前缀), level=20(→8位前缀)
    pub async fn level_sharding_test(
        &mut self,
        count: usize,
    ) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let levels = [5u32, 12u32, 20u32];
        let level_names = ["≤8→base", "8-18→4位", "≥18→8位"];
        let mut success = 0usize;
        let mut fail = 0usize;

        for (idx, &level) in levels.iter().enumerate() {
            let quadkey = match level {
                5 => "30211",
                12 => "302112345678",
                20 => "30211234567890123456",
                _ => unreachable!(),
            };
            println!(
                "    level={} ({}): quadkey={}",
                level, level_names[idx], quadkey
            );

            for i in 0..count {
                let key = format!("lvl_{}_{}", level, i);
                let value = format!("val_level_{}_{}", level, i).into_bytes();
                let req = tonic::Request::new(PutRequest {
                    key: key.clone(),
                    value,
                    content_type: "text/plain".to_string(),
                    tags: String::new(),
                    quadkey: quadkey.to_string(),
                    level,
                    ..Default::default()
                });
                match self.client.put(req).await {
                    Ok(_) => success += 1,
                    Err(e) => {
                        println!("    ✗ 写入失败 level={} key={}: {}", level, key, e);
                        fail += 1;
                    }
                }
            }

            for i in 0..count {
                let key = format!("lvl_{}_{}", level, i);
                let expected = format!("val_level_{}_{}", level, i).into_bytes();
                let req = tonic::Request::new(GetRequest {
                    key: key.clone(),
                    quadkey: quadkey.to_string(),
                    level,
                    ..Default::default()
                });
                match self.client.get(req).await {
                    Ok(resp) => {
                        if resp.into_inner().value == expected {
                            success += 1;
                        } else {
                            println!("    ✗ 值不匹配 level={} key={}", level, key);
                            fail += 1;
                        }
                    }
                    Err(e) => {
                        println!("    ✗ 读取失败 level={} key={}: {}", level, key, e);
                        fail += 1;
                    }
                }
            }
        }

        Ok((success, fail))
    }

    /// 测试 3: 单区域高压写入
    pub async fn region_stress_put(
        &mut self,
        region: &str,
        total: usize,
        concurrency: usize,
        value_size: usize,
    ) -> (f64, f64, usize, usize) {
        let value = vec![b'x'; value_size];
        let client = self.client.clone();
        let start = Instant::now();

        let mut handles = Vec::new();
        let per_worker = total / concurrency;

        for w in 0..concurrency {
            let mut client = client.clone();
            let value = value.clone();
            let region = region.to_string();
            handles.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(per_worker);
                let mut success = 0usize;
                let mut fail = 0usize;
                for i in 0..per_worker {
                    let key = format!("rstress_{}_{}_{}", region, w, i);
                    let req = tonic::Request::new(PutRequest {
                        key,
                        value: value.clone(),
                        content_type: "text/plain".to_string(),
                        tags: String::new(),
                        quadkey: region.clone(),
                        level: 10,
                        ..Default::default()
                    });
                    let s = Instant::now();
                    match client.put(req).await {
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

    /// 测试 4: 全区域混合高压
    pub async fn cross_region_stress(
        &mut self,
        total: usize,
        concurrency: usize,
        value_size: usize,
    ) -> (f64, f64, usize, usize) {
        let value = vec![b'x'; value_size];
        let client = self.client.clone();
        let start = Instant::now();
        let regions = ["0", "1", "2", "3"];

        let mut handles = Vec::new();
        let per_worker = total / concurrency;

        for w in 0..concurrency {
            let mut client = client.clone();
            let value = value.clone();
            handles.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(per_worker);
                let mut success = 0usize;
                let mut fail = 0usize;
                for i in 0..per_worker {
                    let region = regions[(w + i) % 4];
                    let key = format!("crstress_{}_{}", w, i);
                    let req = tonic::Request::new(PutRequest {
                        key,
                        value: value.clone(),
                        content_type: "text/plain".to_string(),
                        tags: String::new(),
                        quadkey: region.to_string(),
                        level: 10,
                        ..Default::default()
                    });
                    let s = Instant::now();
                    match client.put(req).await {
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

    /// 测试 5: 大文件跨区域
    pub async fn large_file_cross_region(
        &mut self,
        count: usize,
        value_size: usize,
    ) -> (f64, f64, usize, usize) {
        let value = vec![b'x'; value_size];
        let client = self.client.clone();
        let start = Instant::now();
        let regions = ["0", "1", "2", "3"];

        let mut handles = Vec::new();
        let per_region = count / 4;

        for (ri, region) in regions.iter().enumerate() {
            let mut client = client.clone();
            let value = value.clone();
            let region = region.to_string();
            handles.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(per_region);
                let mut success = 0usize;
                let mut fail = 0usize;
                for i in 0..per_region {
                    let key = format!("large_{}_{}", ri, i);
                    let req = tonic::Request::new(PutRequest {
                        key,
                        value: value.clone(),
                        content_type: "text/plain".to_string(),
                        tags: String::new(),
                        quadkey: region.clone(),
                        level: 10,
                        ..Default::default()
                    });
                    let s = Instant::now();
                    match client.put(req).await {
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

    /// 测试 6: 跨区域混合读写
    pub async fn cross_region_mixed(
        &mut self,
        read_conc: usize,
        write_conc: usize,
        ops_per_worker: usize,
        value_size: usize,
    ) -> (f64, f64, f64, usize, usize) {
        let value = vec![b'x'; value_size];
        let client = self.client.clone();
        let start = Instant::now();
        let regions = ["0", "1", "2", "3"];

        // 先写入基础数据
        let base_count = read_conc * ops_per_worker;
        for i in 0..base_count {
            let region = regions[i % 4];
            let key = format!("mixed_base_{}", i);
            let req = tonic::Request::new(PutRequest {
                key,
                value: value.clone(),
                content_type: "text/plain".to_string(),
                tags: String::new(),
                quadkey: region.to_string(),
                level: 10,
                ..Default::default()
            });
            let _ = self.client.put(req).await;
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let mut handles = Vec::new();

        // 读任务
        for w in 0..read_conc {
            let mut client = client.clone();
            handles.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(ops_per_worker);
                let mut success = 0usize;
                for i in 0..ops_per_worker {
                    let idx = (w * ops_per_worker + i) % base_count;
                    let region = regions[idx % 4];
                    let key = format!("mixed_base_{}", idx);
                    let req = tonic::Request::new(GetRequest {
                        key,
                        quadkey: region.to_string(),
                        level: 10,
                        ..Default::default()
                    });
                    let s = Instant::now();
                    match client.get(req).await {
                        Ok(_) => {
                            latencies.push(s.elapsed().as_secs_f64() * 1000.0);
                            success += 1;
                        }
                        Err(_) => {}
                    }
                }
                ("read", latencies, success)
            }));
        }

        // 写任务
        for w in 0..write_conc {
            let mut client = client.clone();
            let value = value.clone();
            handles.push(tokio::spawn(async move {
                let mut latencies = Vec::with_capacity(ops_per_worker);
                let mut success = 0usize;
                for i in 0..ops_per_worker {
                    let region = regions[(w + i) % 4];
                    let key = format!("mixed_write_{}_{}", w, i);
                    let req = tonic::Request::new(PutRequest {
                        key,
                        value: value.clone(),
                        content_type: "text/plain".to_string(),
                        tags: String::new(),
                        quadkey: region.to_string(),
                        level: 10,
                        ..Default::default()
                    });
                    let s = Instant::now();
                    match client.put(req).await {
                        Ok(_) => {
                            latencies.push(s.elapsed().as_secs_f64() * 1000.0);
                            success += 1;
                        }
                        Err(_) => {}
                    }
                }
                ("write", latencies, success)
            }));
        }

        let mut read_latencies = Vec::new();
        let mut write_latencies = Vec::new();
        let mut read_success = 0usize;
        let mut write_success = 0usize;

        for h in handles {
            if let Ok((op_type, lats, ok)) = h.await {
                match op_type {
                    "read" => {
                        read_latencies.extend(lats);
                        read_success += ok;
                    }
                    "write" => {
                        write_latencies.extend(lats);
                        write_success += ok;
                    }
                    _ => {}
                }
            }
        }

        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let read_avg = if read_latencies.is_empty() {
            0.0
        } else {
            read_latencies.iter().sum::<f64>() / read_latencies.len() as f64
        };
        let write_avg = if write_latencies.is_empty() {
            0.0
        } else {
            write_latencies.iter().sum::<f64>() / write_latencies.len() as f64
        };

        (read_avg, write_avg, total_ms, read_success, write_success)
    }

    /// 测试 7: 长时间稳定性测试
    pub async fn stability_test_30s(
        &mut self,
        duration_secs: u64,
        concurrency: usize,
        value_size: usize,
    ) -> (u64, u64, f64) {
        let value = vec![b'x'; value_size];
        let client = self.client.clone();
        let start = Instant::now();
        let end = start + std::time::Duration::from_secs(duration_secs);
        let regions = ["0", "1", "2", "3"];

        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let total_count = Arc::new(AtomicU64::new(0));
        let total_bytes = Arc::new(AtomicU64::new(0));

        let mut handles = Vec::new();

        for w in 0..concurrency {
            let mut client = client.clone();
            let value = value.clone();
            let end = end;
            let total_count = total_count.clone();
            let total_bytes = total_bytes.clone();
            handles.push(tokio::spawn(async move {
                let mut i = 0u64;
                while Instant::now() < end {
                    let region = regions[(w as usize + i as usize) % 4];
                    let key = format!("stab_{}_{}", w, i);
                    let req = tonic::Request::new(PutRequest {
                        key,
                        value: value.clone(),
                        content_type: "text/plain".to_string(),
                        tags: String::new(),
                        quadkey: region.to_string(),
                        level: 10,
                        ..Default::default()
                    });
                    match client.put(req).await {
                        Ok(_) => {
                            total_count.fetch_add(1, Ordering::Relaxed);
                            total_bytes.fetch_add(value_size as u64, Ordering::Relaxed);
                        }
                        Err(_) => {}
                    }
                    i += 1;
                }
            }));
        }

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
