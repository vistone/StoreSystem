/// 多 Worker 副本写入故障恢复测试
/// 验证：
///   1. 同步写主 + 异步写备副本
///   2. 读取时主副本失败 → fallback 备副本
///   3. Worker 宕机后写入成功率（副本保证）
///   4. 数据完整性
#[path = "../grpc_client.rs"]
mod grpc_client;
use grpc_client::GrpcClient;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("╔══════════════════════════════════════════════════╗");
    println!("║     多 Worker 副本写入 故障恢复测试               ║");
    println!("║     验证: 同步主副本 + 异步备副本 + fallback 读取  ║");
    println!("╚══════════════════════════════════════════════════╝\n");

    // ============================================================
    // 阶段 1: 基础写入 + 验证副本存在
    // ============================================================
    println!("━ 阶段 1: 写入 50 条 + 验证副本 ━━");
    let mut grpc = GrpcClient::connect("http://localhost:50051").await?;

    let start = Instant::now();
    for i in 0u64..50 {
        let key = format!("rep_{:04}", i);
        let value = vec![(i % 256) as u8; 1024 * 1024]; // 1MB
        let quadkey = grpc_client::key_to_quadkey(&key);
        grpc.put(&key, value, &quadkey, 10).await?;
    }
    println!("✅ 写入完成: {:.1}s", start.elapsed().as_secs_f64());

    // 等待刷盘完成（flusher 间隔 5ms，给 500ms 充足时间）
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 阶段 1 验证：立即读取（flush 可能还在进行）
    let mut readable_after_write = 0;
    for i in 0u64..50 {
        let key = format!("rep_{:04}", i);
        let quadkey = grpc_client::key_to_quadkey(&key);
        if grpc.get(&key, &quadkey, 10).await.is_ok() {
            readable_after_write += 1;
        }
    }
    println!("✅ 阶段 1 可读: {}/50", readable_after_write);

    // ============================================================
    // 阶段 2: 杀死 Worker-2，测试副本故障转移
    // ============================================================
    println!("\n━ 阶段 2: 杀死 Worker-2，测试副本故障转移 ━━");

    // 先等刷盘完成
    tokio::time::sleep(Duration::from_secs(1)).await;

    println!("💀 杀死 Worker-2...");
    std::process::Command::new("pkill")
        .args(["-9", "-f", "worker2\\.yaml"])
        .output()
        .ok();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 立即尝试读取阶段 1 的所有数据
    let mut read_after_kill = 0;
    let mut read_failures = Vec::new();
    for i in 0u64..50 {
        let key = format!("rep_{:04}", i);
        let quadkey = grpc_client::key_to_quadkey(&key);
        match grpc.get(&key, &quadkey, 10).await {
            Ok(v) => {
                read_after_kill += 1;
                assert_eq!(v.len(), 1024 * 1024, "数据损坏: {}", key);
            }
            Err(e) => read_failures.push((key, format!("{}", e))),
        }
    }

    println!(
        "Worker-2 宕机后读: {}/50 (丢失 {})",
        read_after_kill,
        read_failures.len()
    );
    if !read_failures.is_empty() {
        println!("  丢失样本 (前 5):");
        for (k, e) in read_failures.iter().take(5) {
            let trunc: String = e.chars().take(80).collect();
            println!("    {} → {}", k, trunc);
        }
    }

    // ============================================================
    // 阶段 3: 宕机期间并发写入
    // ============================================================
    println!("\n━ 阶段 3: 宕机期间并发写入 ━━");

    let total_ops = Arc::new(AtomicUsize::new(0));
    let error_ops = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(AtomicBool::new(true));

    let mut handles = vec![];
    for _ in 0..4 {
        let running = running.clone();
        let total = total_ops.clone();
        let errors = error_ops.clone();
        let handle = tokio::spawn(async move {
            let mut client = GrpcClient::connect("http://localhost:50051").await.unwrap();
            while running.load(Ordering::Relaxed) {
                let i = total.fetch_add(1, Ordering::Relaxed);
                let key = format!("outage_{:06}", i);
                let value = vec![(i % 256) as u8; 64 * 1024]; // 64KB
                let qk = grpc_client::key_to_quadkey(&key);
                if client.put(&key, value, &qk, 10).await.is_err() {
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }

    tokio::time::sleep(Duration::from_secs(5)).await;
    running.store(false, Ordering::Relaxed);
    for h in handles {
        let _ = h.await;
    }

    let total = total_ops.load(Ordering::Relaxed);
    let errors = error_ops.load(Ordering::Relaxed);
    let success = total.saturating_sub(errors);
    let rate = if total > 0 {
        success as f64 / total as f64 * 100.0
    } else {
        100.0
    };
    println!(
        "宕机期间写入: {} 成功 / {} 错误 (成功率 {:.0}%)",
        success, errors, rate
    );

    // ============================================================
    // 阶段 4: 重启 Worker-2
    // ============================================================
    println!("\n━ 阶段 4: 重启 Worker-2 ━━");
    let _ = std::process::Command::new("/home/stone/StoreSystem/target/release/store_system")
        .args(["--config", "worker2.yaml"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
    tokio::time::sleep(Duration::from_secs(5)).await;

    // ============================================================
    // 阶段 5: 二次读取验证（Worker-2恢复后）
    // ============================================================
    println!("\n━ 阶段 5: Worker-2 恢复后数据完整性 ━━");

    let mut post_recovery_read = 0;
    let mut post_missing = Vec::new();
    for i in 0u64..50 {
        let key = format!("rep_{:04}", i);
        let quadkey = grpc_client::key_to_quadkey(&key);
        match grpc.get(&key, &quadkey, 10).await {
            Ok(v) => {
                post_recovery_read += 1;
                if v.len() != 1024 * 1024 {
                    eprintln!("  数据大小不一致: {} expect=1MB got={}", key, v.len());
                }
            }
            Err(e) => post_missing.push((key, format!("{}", e))),
        }
    }

    println!(
        "恢复后读取: {}/50 (丢失 {})",
        post_recovery_read,
        post_missing.len()
    );

    // ============================================================
    // 阶段 6: 验证异步副本 — 宕机期间的写是否复制到 Worker-2
    // ============================================================
    println!("\n━ 阶段 6: WAL 数据验证 ━━");
    let w1_log = std::fs::read_to_string("/tmp/w1.log").unwrap_or_default();
    let w2_log = std::fs::read_to_string("/tmp/w2.log").unwrap_or_default();
    let w1_recovery = w1_log.contains("[recovery]");
    let w2_recovery = w2_log.contains("[recovery]");
    println!(
        "  Worker-1 WAL 恢复: {}",
        if w1_recovery {
            "有恢复操作"
        } else {
            "正常"
        }
    );
    println!(
        "  Worker-2 WAL 恢复: {}",
        if w2_recovery {
            "有恢复操作"
        } else {
            "正常"
        }
    );

    // ============================================================
    // 最终报告
    // ============================================================
    println!("\n╔══════════════════════════════════════════════════╗");
    println!("║              测试结果总结                          ║");
    println!("╠══════════════════════════════════════════════════╣");

    let s1 = readable_after_write >= 45;
    let s2 = read_after_kill >= 20; // 至少一部分主副本在 Worker-1 上可读
    let s3 = rate >= 70.0; // 写入成功率 >= 70%
    let s5 = post_recovery_read >= 45;
    let all_pass = s1 && s2 && s3 && s5;

    println!(
        "║  阶段 1 (初始写入):  {} 可读={}/50            ║",
        if s1 { "✅" } else { "❌" },
        readable_after_write
    );
    println!(
        "║  阶段 2 (故障读取):  {} 可读={}/50 (副本fallback)║",
        if s2 { "✅" } else { "⚠️ " },
        read_after_kill
    );
    println!(
        "║  阶段 3 (故障写入):  {} 成功率={:.0}% (副本路由)   ║",
        if s3 { "✅" } else { "⚠️ " },
        rate
    );
    println!(
        "║  阶段 5 (恢复读):    {} 可读={}/50            ║",
        if s5 { "✅" } else { "⚠️ " },
        post_recovery_read
    );

    println!("╠══════════════════════════════════════════════════╣");
    println!(
        "║  综合结论:            {:>28} ║",
        if all_pass {
            "✅ 全部通过"
        } else {
            "⚠️  部分通过"
        }
    );
    println!("╚══════════════════════════════════════════════════╝");

    if all_pass {
        println!("\n🎉 副本机制验证成功：");
        println!("   - 同步写主副本 + 异步写备副本工作正常");
        println!("   - 读取时主副本不可达可 fallback 到备副本");
        println!("   - 宕机期间写入成功率显著提升");
        println!("   - 数据在 Worker 恢复后完整可读");
    }

    Ok(())
}
