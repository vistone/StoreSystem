mod grpc_client;
mod restful_client;

use std::time::Instant;

#[derive(Debug, Clone)]
struct BenchResult {
    name: String,
    rounds: usize,
    avg_latency_ms: f64,
    throughput_ops: f64,
    total_ms: f64,
    /// 吞吐量 MB/s
    throughput_mbs: f64,
}

impl BenchResult {
    fn new(name: &str, rounds: usize, avg_latency_ms: f64, total_ms: f64, total_bytes: u64) -> Self {
        let throughput_ops = if total_ms > 0.0 {
            (rounds as f64 / total_ms) * 1000.0
        } else {
            0.0
        };
        let throughput_mbs = if total_ms > 0.0 {
            (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0)
        } else {
            0.0
        };
        Self {
            name: name.to_string(),
            rounds,
            avg_latency_ms,
            throughput_ops,
            total_ms,
            throughput_mbs,
        }
    }
}

struct Report {
    results: Vec<BenchResult>,
}

impl Report {
    fn new() -> Self {
        Self { results: Vec::new() }
    }

    fn add(&mut self, r: BenchResult) {
        self.results.push(r);
    }

    fn print(&self) {
        println!();
        println!("==========================================================================================================");
        println!("                              存储系统性能测试报告");
        println!("==========================================================================================================");
        println!("{:<44} {:>6} {:>12} {:>12} {:>12} {:>12}", "测试项", "次数", "平均延迟(ms)", "总耗时(ms)", "ops/s", "MB/s");
        println!("----------------------------------------------------------------------------------------------------------");
        for r in &self.results {
            println!(
                "{:<44} {:>6} {:>12.3} {:>12.3} {:>12.1} {:>12.1}",
                r.name, r.rounds, r.avg_latency_ms, r.total_ms, r.throughput_ops, r.throughput_mbs
            );
        }
        println!("==========================================================================================================");
        println!();
    }
}

async fn wait_for_server() {
    println!("等待服务启动...");
    for i in 0..30 {
        if reqwest::get("http://localhost:52061/objects?limit=1").await.is_ok() {
            println!("RESTful 服务已就绪");
            break;
        }
        if i == 29 {
            println!("警告: 无法连接 RESTful 服务，请确认服务已启动");
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

fn fmt_size(bytes: usize) -> String {
    if bytes >= 1024 * 1024 {
        format!("{}MB", bytes / 1024 / 1024)
    } else if bytes >= 1024 {
        format!("{}KB", bytes / 1024)
    } else {
        format!("{}B", bytes)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 存储系统大 Value 性能测试客户端启动");
    println!("========================================");
    // 分布式模式：gRPC 连接 Master(50051)，RESTful 连接 Worker(52061)
    println!("gRPC 服务地址: http://localhost:50051");
    println!("RESTful 服务地址: http://localhost:52061");
    println!();

    wait_for_server().await;

    let mut report = Report::new();

    // ============ gRPC 测试 ============
    println!("\n📡 开始 gRPC 性能测试...");
    let mut grpc = match grpc_client::GrpcClient::connect("http://localhost:50051").await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("❌ gRPC 连接失败: {}", e);
            eprintln!("   跳过 gRPC 测试");
            return run_restful_only(report).await;
        }
    };

    // 不同 value 大小的单次写入测试
    // 1KB, 1MB, 10MB, 50MB, 100MB
    let value_sizes: Vec<(usize, usize)> = vec![
        (1024, 20),           // 1KB x 20轮
        (1024 * 1024, 10),    // 1MB x 10轮
        (10 * 1024 * 1024, 5), // 10MB x 5轮
        (50 * 1024 * 1024, 3), // 50MB x 3轮
        (100 * 1024 * 1024, 2), // 100MB x 2轮
    ];

    for (size, rounds) in &value_sizes {
        let label = format!("gRPC 单次写入 ({})", fmt_size(*size));
        println!("  ▶ 测试 {} ({}轮)...", label, rounds);
        let start = Instant::now();
        let avg = grpc.put_bench(*rounds, *size).await?;
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let total_bytes = (*size * rounds) as u64;
        report.add(BenchResult::new(&label, *rounds, avg, total_ms, total_bytes));
        println!("    ✓ 平均延迟: {:.3}ms, 吞吐: {:.1} ops/s, {:.1} MB/s", avg, (*rounds as f64 / total_ms) * 1000.0, (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0));
    }

    // gRPC 单次读取测试（读取之前写入的大 value）
    println!("\n  ▶ 测试 gRPC 大 Value 读取...");
    for (size, rounds) in &value_sizes {
        // 先写入数据
        for i in 0..*rounds {
            let key = format!("grpc_read_{}_{}", size, i);
            let _ = grpc.put_single(&key, vec![b'x'; *size]).await;
        }
        // 等待后台刷盘完成：flusher 收到 notify 后立即处理
        // 根据 value 大小动态调整等待时间（每 1MB 给 10ms，最少 100ms，最多 500ms）
        let wait_ms = ((*size as u64 / (1024 * 1024)) * 10).max(100).min(500);
        tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
        let label = format!("gRPC 单次读取 ({})", fmt_size(*size));
        println!("    ▶ {} ({}轮)...", label, rounds);
        let mut total_ms = 0.0;
        for i in 0..*rounds {
            let key = format!("grpc_read_{}_{}", size, i);
            let start = Instant::now();
            let _ = grpc.get_single(&key).await?;
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }
        let avg = total_ms / *rounds as f64;
        let total_bytes = (*size * rounds) as u64;
        report.add(BenchResult::new(&label, *rounds, avg, total_ms, total_bytes));
        println!("    ✓ 平均延迟: {:.3}ms, 吞吐: {:.1} MB/s", avg, (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0));
    }

    // gRPC 批量写入测试（大 value）
    // 1MB x 10条/批, 10MB x 5条/批
    let batch_tests: Vec<(usize, usize, usize)> = vec![
        (1024 * 1024, 10, 5),    // 1MB x 10条, 5轮
        (10 * 1024 * 1024, 5, 3), // 10MB x 5条, 3轮
    ];
    for (size, batch_size, rounds) in &batch_tests {
        let label = format!("gRPC 批量写入 ({} x {}条/批)", fmt_size(*size), batch_size);
        println!("  ▶ 测试 {} ({}轮)...", label, rounds);
        let start = Instant::now();
        let avg = grpc.put_batch_bench(*rounds, *batch_size, *size).await?;
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let total_bytes = (*size * batch_size * rounds) as u64;
        report.add(BenchResult::new(&label, *rounds, avg, total_ms, total_bytes));
        println!("    ✓ 平均延迟: {:.3}ms, 吞吐: {:.1} MB/s (含 {} 条)", avg, (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0), batch_size * rounds);
    }

    // gRPC 并发写入测试（大 value）
    // 1MB x 10并发, 10MB x 5并发
    let conc_tests: Vec<(usize, usize, usize)> = vec![
        (1024 * 1024, 10, 20),    // 1MB, 10并发, 20总请求
        (10 * 1024 * 1024, 5, 10), // 10MB, 5并发, 10总请求
    ];
    for (size, conc, total) in &conc_tests {
        let label = format!("gRPC 并发写入 ({} x {}并发)", fmt_size(*size), conc);
        println!("  ▶ 测试 {} ({}总请求)...", label, total);
        let (avg, total_ms) = grpc.put_concurrent_bench(*total, *conc, *size).await?;
        let total_bytes = (*size * total) as u64;
        report.add(BenchResult::new(&label, *total, avg, total_ms, total_bytes));
        println!("    ✓ 平均延迟: {:.3}ms, 总耗时: {:.3}ms, 吞吐: {:.1} MB/s", avg, total_ms, (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0));
    }

    // ============ RESTful 测试 ============
    println!("\n🌐 开始 RESTful API 性能测试...");
    let rest = restful_client::RestfulClient::new("http://localhost:52061", "objects");
    for (size, rounds) in &value_sizes {
        let label = format!("RESTful 单次写入 ({})", fmt_size(*size));
        println!("  ▶ 测试 {} ({}轮)...", label, rounds);
        let start = Instant::now();
        let avg = rest.put_bench(*rounds, *size).await?;
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let total_bytes = (*size * rounds) as u64;
        report.add(BenchResult::new(&label, *rounds, avg, total_ms, total_bytes));
        println!("    ✓ 平均延迟: {:.3}ms, 吞吐: {:.1} ops/s, {:.1} MB/s", avg, (*rounds as f64 / total_ms) * 1000.0, (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0));
    }

    // RESTful 单次读取测试（大 value）
    println!("\n  ▶ 测试 RESTful 大 Value 读取...");
    for (size, rounds) in &value_sizes {
        // 先写入数据
        for i in 0..*rounds {
            let key = format!("rest_read_{}_{}", size, i);
            let _ = rest.put_single(&key, vec![b'x'; *size]).await;
        }
        let label = format!("RESTful 单次读取 ({})", fmt_size(*size));
        println!("    ▶ {} ({}轮)...", label, rounds);
        let mut total_ms = 0.0;
        for i in 0..*rounds {
            let key = format!("rest_read_{}_{}", size, i);
            let start = Instant::now();
            let _ = rest.get_single(&key).await;
            total_ms += start.elapsed().as_secs_f64() * 1000.0;
        }
        let avg = total_ms / *rounds as f64;
        let total_bytes = (*size * rounds) as u64;
        report.add(BenchResult::new(&label, *rounds, avg, total_ms, total_bytes));
        println!("    ✓ 平均延迟: {:.3}ms, 吞吐: {:.1} MB/s", avg, (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0));
    }

    // RESTful 批量写入测试（大 value）
    for (size, batch_size, rounds) in &batch_tests {
        let label = format!("RESTful 批量写入 ({} x {}条/批)", fmt_size(*size), batch_size);
        println!("  ▶ 测试 {} ({}轮)...", label, rounds);
        let start = Instant::now();
        let avg = rest.put_batch_bench(*rounds, *batch_size, *size).await?;
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let total_bytes = (*size * batch_size * rounds) as u64;
        report.add(BenchResult::new(&label, *rounds, avg, total_ms, total_bytes));
        println!("    ✓ 平均延迟: {:.3}ms, 吞吐: {:.1} MB/s (含 {} 条)", avg, (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0), batch_size * rounds);
    }

    // RESTful 并发写入测试（大 value）
    for (size, conc, total) in &conc_tests {
        let label = format!("RESTful 并发写入 ({} x {}并发)", fmt_size(*size), conc);
        println!("  ▶ 测试 {} ({}总请求)...", label, total);
        let (avg, total_ms) = rest.put_concurrent_bench(*total, *conc, *size).await?;
        let total_bytes = (*size * total) as u64;
        report.add(BenchResult::new(&label, *total, avg, total_ms, total_bytes));
        println!("    ✓ 平均延迟: {:.3}ms, 总耗时: {:.3}ms, 吞吐: {:.1} MB/s", avg, total_ms, (total_bytes as f64 / 1024.0 / 1024.0) / (total_ms / 1000.0));
    }

    // 打印报告
    report.print();

    println!("\n✅ 大 Value 性能测试完成！");
    Ok(())
}

async fn run_restful_only(mut report: Report) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🌐 仅运行 RESTful API 性能测试...");
    let rest = restful_client::RestfulClient::new("http://localhost:52061", "objects");

    let value_sizes: Vec<(usize, usize)> = vec![
        (1024, 20),
        (1024 * 1024, 10),
        (10 * 1024 * 1024, 5),
        (50 * 1024 * 1024, 3),
        (100 * 1024 * 1024, 2),
    ];

    for (size, rounds) in &value_sizes {
        let label = format!("RESTful 单次写入 ({})", fmt_size(*size));
        println!("  ▶ 测试 {} ({}轮)...", label, rounds);
        let start = Instant::now();
        let avg = rest.put_bench(*rounds, *size).await?;
        let total_ms = start.elapsed().as_secs_f64() * 1000.0;
        let total_bytes = (*size * rounds) as u64;
        report.add(BenchResult::new(&label, *rounds, avg, total_ms, total_bytes));
    }

    report.print();
    println!("\n✅ 大 Value 性能测试完成！");
    Ok(())
}
