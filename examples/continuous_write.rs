use std::time::{Duration, Instant};
use store_system::grpc::proto::store_service_client::StoreServiceClient;
use store_system::grpc::proto::{BatchItem, PutBatchRequest};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = StoreServiceClient::connect("http://localhost:50051")
        .await?
        .max_decoding_message_size(256 * 1024 * 1024)
        .max_encoding_message_size(256 * 1024 * 1024);

    println!("=== 高速写入测试（并发 + 批量）===");
    println!("模式: 10 并发，每批 50 条，Ctrl+C 停止\n");

    let (tx, mut rx) = mpsc::channel::<(u64, u64)>(1000); // (count, bytes)

    // 启动 10 个并发写入任务
    let concurrency = 10;
    let batch_size = 10;
    let value_sizes: Vec<usize> = vec![1024, 1024 * 1024]; // 去掉 10MB，避免单批过大

    for worker_id in 0..concurrency {
        let mut store = store.clone();
        let tx = tx.clone();
        let sizes = value_sizes.clone();

        tokio::spawn(async move {
            let mut key_idx: u64 = 0;
            let mut size_idx = 0;

            loop {
                let size = sizes[size_idx];
                let value = vec![b'x'; size];

                // 构建批量请求
                let items: Vec<BatchItem> = (0..batch_size)
                    .map(|i| {
                        let key = format!("hpc_{}_{}_{}", worker_id, key_idx, i);
                        BatchItem {
                            key,
                            value: value.clone(),
                            content_type: "text/plain".to_string(),
                            tags: String::new(),
                            ..Default::default()
                        }
                    })
                    .collect();

                let batch_bytes = (size * batch_size) as u64;
                let req = PutBatchRequest { items };

                match store.put_batch(req).await {
                    Ok(_) => {
                        let _ = tx.send((batch_size as u64, batch_bytes)).await;
                    }
                    Err(e) => {
                        eprintln!("worker {} 批量写入失败: {}", worker_id, e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }

                key_idx += 1;
                size_idx = (size_idx + 1) % sizes.len();
            }
        });
    }

    drop(tx); // 关闭发送端，rx 会在所有任务结束后关闭

    // 统计任务
    let mut total_count: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut interval_count: u64 = 0;
    let mut interval_bytes: u64 = 0;
    let interval = Duration::from_secs(1);
    let mut last_report = Instant::now();
    let start = Instant::now();

    while let Some((count, bytes)) = rx.recv().await {
        total_count += count;
        total_bytes += bytes;
        interval_count += count;
        interval_bytes += bytes;

        if last_report.elapsed() >= interval {
            let elapsed = last_report.elapsed().as_secs_f64();
            let total_elapsed = start.elapsed().as_secs_f64();
            let ops = interval_count as f64 / elapsed;
            let mbs = (interval_bytes as f64 / 1024.0 / 1024.0) / elapsed;

            println!(
                "[{:.0}s] 本秒: {:.0} ops/s, {:.1} MB/s | 累计: {} 条, {:.2} MB, 平均 {:.0} ops/s",
                total_elapsed,
                ops,
                mbs,
                total_count,
                total_bytes as f64 / 1024.0 / 1024.0,
                total_count as f64 / total_elapsed
            );

            interval_count = 0;
            interval_bytes = 0;
            last_report = Instant::now();
        }
    }

    Ok(())
}
