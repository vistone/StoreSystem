use std::time::Instant;
use store_system::grpc::proto::store_service_client::StoreServiceClient;
use store_system::grpc::proto::PutRequest;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut store = StoreServiceClient::connect("http://localhost:50051")
        .await?
        .max_decoding_message_size(256 * 1024 * 1024)
        .max_encoding_message_size(256 * 1024 * 1024);

    println!("=== 各环节耗时测量 ===\n");

    // 测试不同大小的写入耗时
    for &size in &[1024, 1024 * 1024, 10 * 1024 * 1024] {
        let value = vec![b'x'; size];
        let rounds = 20;

        // 预热
        let _ = store
            .put(PutRequest {
                key: "warmup".to_string(),
                value: value.clone(),
                content_type: "text/plain".to_string(),
                tags: String::new(),
            })
            .await?;

        // 测量 master->worker->buffer 的端到端延迟
        let mut times = Vec::new();
        for i in 0..rounds {
            let key = format!("latency_test_{}_{}", size, i);
            let start = Instant::now();
            store
                .put(PutRequest {
                    key,
                    value: value.clone(),
                    content_type: "text/plain".to_string(),
                    tags: String::new(),
                })
                .await?;
            times.push(start.elapsed());
        }

        let total: std::time::Duration = times.iter().sum();
        let avg = total / rounds as u32;
        let min = times.iter().min().expect("times is non-empty");
        let max = times.iter().max().expect("times is non-empty");
        let p50 = &times[rounds / 2];

        println!("数据大小: {} KB", size / 1024);
        println!(
            "  平均: {:.2}ms  P50: {:.2}ms  最小: {:.2}ms  最大: {:.2}ms",
            avg.as_secs_f64() * 1000.0,
            p50.as_secs_f64() * 1000.0,
            min.as_secs_f64() * 1000.0,
            max.as_secs_f64() * 1000.0
        );
        println!(
            "  吞吐: {:.0} ops/s, {:.1} MB/s",
            1000.0 / (avg.as_secs_f64() * 1000.0),
            (size as f64 / 1024.0 / 1024.0) / avg.as_secs_f64()
        );
        println!();
    }

    // 测试并发写入
    println!("=== 并发写入测试（10 并发）===");
    let value = vec![b'x'; 1024];
    let start = Instant::now();
    let mut handles = Vec::new();
    for i in 0..10 {
        let mut store = StoreServiceClient::connect("http://localhost:50051")
            .await?
            .max_decoding_message_size(256 * 1024 * 1024)
            .max_encoding_message_size(256 * 1024 * 1024);
        let value = value.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("conc_{}", i);
            let s = Instant::now();
            store
                .put(PutRequest {
                    key,
                    value,
                    content_type: "text/plain".to_string(),
                    tags: String::new(),
                })
                .await
                .expect("concurrent put failed");
            s.elapsed()
        }));
    }
    let mut conc_times = Vec::new();
    for h in handles {
        conc_times.push(h.await?);
    }
    let conc_total = start.elapsed();
    println!(
        "  10 条并发总耗时: {:.2}ms",
        conc_total.as_secs_f64() * 1000.0
    );
    println!(
        "  平均每条: {:.2}ms",
        conc_total.as_secs_f64() * 1000.0 / 10.0
    );

    Ok(())
}
