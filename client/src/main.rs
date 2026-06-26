mod grpc_client;
mod restful_client;

/// 打印分隔线
fn print_separator(title: &str) {
    println!("\n{}", "=".repeat(70));
    println!("  {}", title);
    println!("{}", "=".repeat(70));
}

/// 打印测试结果
fn print_result(label: &str, avg_ms: f64, total_ms: f64, success: usize, fail: usize) {
    let total_sec = total_ms / 1000.0;
    let ops = if total_sec > 0.0 {
        success as f64 / total_sec
    } else {
        0.0
    };
    println!(
        "  {}: 平均={:.2}ms, 总耗时={:.2}s, 成功={}, 失败={}, {:.0} ops/s",
        label, avg_ms, total_sec, success, fail, ops
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", "=".repeat(70));
    println!("  StoreSystem QuadKey 高压测试工具 v0.1.0");
    println!("  gRPC → Master: http://localhost:50051");
    println!("  QuadKey 区域路由: 0/1/2/3 → Worker-0/1/2/3");
    println!("  QuadKey 分片: ≤8→base, 8-18→4位前缀, ≥18→8位前缀");
    println!("{}", "=".repeat(70));

    let mut grpc = grpc_client::GrpcClient::connect("http://localhost:50051").await?;

    // ============================================================
    // 测试 1: 4 区域路由验证
    // ============================================================
    print_separator("测试 1: 4 区域路由验证（各 100 条，写入+读取验证）");
    let (ok, fail) = grpc.quadkey_routing_stress_test(100).await?;
    println!("  结果: 成功 {} 次, 失败 {} 次", ok, fail);
    if fail > 0 {
        eprintln!("  ⚠ 路由验证失败！");
    }

    // ============================================================
    // 测试 2: 3 级分片验证
    // ============================================================
    print_separator("测试 2: 3 级分片验证（level=5/12/20，各 50 条）");
    let (ok, fail) = grpc.level_sharding_test(50).await?;
    println!("  结果: 成功 {} 次, 失败 {} 次", ok, fail);
    if fail > 0 {
        eprintln!("  ⚠ 分片验证失败！");
    }

    // ============================================================
    // 测试 3: 单区域高压写入
    // ============================================================
    print_separator("测试 3: 单区域高压写入（50并发×1000条×1KB，每个区域独立测试）");
    for ridx in 0..4u32 {
        let quadkey = format!("{:04}", ridx);
        let region = format!("{}", ridx);
        let (avg, total, ok, fail) = grpc.region_stress_put(&quadkey, 1000, 50, 1024).await;
        print_result(&format!("区域 {} 高压写入", region), avg, total, ok, fail);
    }

    // ============================================================
    // 测试 4: 全区域混合高压
    // ============================================================
    print_separator("测试 4: 全区域混合高压（50并发×4000条×1KB，均匀分布到 4 区域）");
    let (avg, total, ok, fail) = grpc.cross_region_stress(4000, 50, 1024).await;
    print_result("全区域混合高压", avg, total, ok, fail);

    // ============================================================
    // 测试 5: 大文件跨区域
    // ============================================================
    print_separator("测试 5: 大文件跨区域（1MB 文件，4 区域各 100 条）");
    let (avg, total, ok, fail) = grpc.large_file_cross_region(400, 1024 * 1024).await;
    print_result("大文件跨区域", avg, total, ok, fail);

    // ============================================================
    // 测试 6: 跨区域混合读写
    // ============================================================
    print_separator("测试 6: 跨区域混合读写（20读+20写并发，随机区域）");
    let (read_avg, write_avg, total, read_ok, write_ok) =
        grpc.cross_region_mixed(20, 20, 50, 1024).await;
    let total_sec = total / 1000.0;
    println!(
        "  读: 平均={:.2}ms, 成功={}, {:.0} ops/s",
        read_avg,
        read_ok,
        if total_sec > 0.0 {
            read_ok as f64 / total_sec
        } else {
            0.0
        }
    );
    println!(
        "  写: 平均={:.2}ms, 成功={}, {:.0} ops/s",
        write_avg,
        write_ok,
        if total_sec > 0.0 {
            write_ok as f64 / total_sec
        } else {
            0.0
        }
    );
    println!("  总耗时: {:.2}s", total_sec);

    // ============================================================
    // 测试 7: 长时间稳定性测试
    // ============================================================
    print_separator("测试 7: 长时间稳定性测试（10并发×30秒，随机区域）");
    let (count, bytes, elapsed) = grpc.stability_test_30s(30, 10, 1024).await;
    let avg_ops = if elapsed > 0.0 {
        count as f64 / elapsed
    } else {
        0.0
    };
    let avg_mbs = if elapsed > 0.0 {
        (bytes as f64 / 1024.0 / 1024.0) / elapsed
    } else {
        0.0
    };
    println!(
        "  稳定性: {} 条, {:.1} MB, {:.1}s, {:.0} ops/s, {:.1} MB/s",
        count,
        bytes as f64 / 1024.0 / 1024.0,
        elapsed,
        avg_ops,
        avg_mbs
    );

    // ============================================================
    // 总结
    // ============================================================
    print_separator("测试完成");
    println!("  所有 QuadKey 高压测试已完成。");
    println!("  gRPC 路径: Client → Master(:50051) → Worker(quadkey[0]区域路由)");
    println!("  分片路径: quad_data/objects/{{level}}/{{prefix}}.kv");
    println!("  覆盖: 4 区域路由 ✓ 3 级分片 ✓ 高压写入 ✓ 混合读写 ✓ 稳定性 ✓");

    Ok(())
}
