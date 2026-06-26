mod config;
mod policy;
mod prober;
mod process;

use config::GuardianConfig;
use policy::handle_dead;
use prober::{probe_master, probe_worker, ProbeResult};
use process::{GuardedProcess, ProcessState};
use std::collections::BTreeMap;
use std::time::Duration;

/// gRPC proto 定义（由 build.rs 编译生成）
pub mod proto {
    tonic::include_proto!("store");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    let config_path = args
        .iter()
        .position(|a| a == "--config")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("guardian.yaml");

    let role_filter: Option<String> = args
        .iter()
        .position(|a| a == "--role")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let config = GuardianConfig::load(config_path)?;
    let settings = config.guardian.clone();

    // 过滤进程
    let processes: BTreeMap<String, _> = if let Some(ref role) = role_filter {
        config
            .processes
            .into_iter()
            .filter(|(name, _)| {
                if role == "master" {
                    name == "master"
                } else if role == "worker" {
                    name != "master"
                } else {
                    true
                }
            })
            .collect()
    } else {
        config.processes
    };

    if processes.is_empty() {
        eprintln!("[guardian] 没有匹配的进程");
        return Ok(());
    }

    eprintln!(
        "[guardian] 启动守护, 管理 {} 个进程, 探针间隔={}s, 失败阈值={}",
        processes.len(),
        settings.probe_interval_secs,
        settings.failure_threshold
    );

    // 按 depends_on 拓扑排序启动
    let ordered: Vec<String> = topological_sort(&processes);

    let mut guarded: BTreeMap<String, GuardedProcess> = BTreeMap::new();
    for name in &ordered {
        let cfg = processes[name].clone();
        let mut proc = GuardedProcess::new(name.clone(), cfg);

        // 等待依赖进程 Running
        if let Some(ref dep) = proc.config.depends_on {
            if let Some(dep_proc) = guarded.get(dep) {
                eprintln!("[guardian] {} 等待依赖 {} 就绪...", name, dep);
                wait_for_running(dep_proc, &settings).await;
            }
        }

        proc.spawn()?;
        // 等待自身健康
        eprintln!("[guardian] 等待 {} 健康检查...", name);
        wait_for_running(&proc, &settings).await;
        guarded.insert(name.clone(), proc);
    }

    eprintln!("[guardian] ✅ 所有进程就绪，进入监控循环");

    // 监控循环
    loop {
        tokio::time::sleep(Duration::from_secs(settings.probe_interval_secs)).await;

        for name in ordered.clone() {
            let proc = guarded.get_mut(&name).unwrap();
            let is_master = name == "master";

            // 冷却中 → 跳过
            if proc.state == ProcessState::Cooldown {
                if !proc.is_in_cooldown() {
                    // 冷却结束，重置
                    eprintln!("[guardian] {} 冷却结束，重置失败计数", name);
                    proc.cooldown_until = None;
                    proc.failures = 0;
                    if proc.spawn().is_ok() {
                        // 等待健康
                        let _temp_proc = proc;
                    }
                }
                continue;
            }

            // 检查 PID
            if !proc.is_pid_alive() {
                eprintln!("[guardian] {} PID 不存在, 标记 Dead", name);
                proc.state = ProcessState::Dead;
                proc.failures = proc.failures.saturating_add(1);
                proc.last_failure = Some(std::time::Instant::now());
                if let Some(delay) = handle_dead(proc, &settings) {
                    tokio::time::sleep(delay).await;
                    let _ = proc.spawn();
                }
                continue;
            }

            // 深度探针
            let health_addr = &proc.config.health_grpc.clone();
            let timeout = settings.probe_timeout_secs;
            let result = if is_master {
                probe_master(health_addr, timeout).await
            } else {
                probe_worker(health_addr, timeout).await
            };

            match result {
                ProbeResult::Running => {
                    if proc.state != ProcessState::Running {
                        eprintln!("[guardian] {} 恢复健康", name);
                    }
                    proc.state = ProcessState::Running;
                    proc.failures = 0;
                }
                result @ (ProbeResult::Degraded(_)
                | ProbeResult::ConnectionRefused
                | ProbeResult::Timeout) => {
                    proc.failures = proc.failures.saturating_add(1);
                    proc.last_failure = Some(std::time::Instant::now());
                    let desc = match &result {
                        ProbeResult::Degraded(ref m) => m.as_str(),
                        ProbeResult::ConnectionRefused => "connection refused",
                        ProbeResult::Timeout => "timeout",
                        _ => "unknown",
                    };
                    eprintln!(
                        "[guardian] {} 探针失败 ({}/{}): {}",
                        name, proc.failures, settings.failure_threshold, desc
                    );

                    if proc.failures >= settings.failure_threshold {
                        proc.state = ProcessState::Dead;
                        eprintln!("[guardian] {} 连续失败, 标记 Dead", name);
                        if let Some(delay) = handle_dead(proc, &settings) {
                            tokio::time::sleep(delay).await;
                            let _ = proc.spawn();
                        }
                    } else {
                        proc.state = ProcessState::Degraded;
                    }
                }
                ProbeResult::ProcessGone => {
                    proc.state = ProcessState::Dead;
                    proc.failures = proc.failures.saturating_add(1);
                    if let Some(delay) = handle_dead(proc, &settings) {
                        tokio::time::sleep(delay).await;
                        let _ = proc.spawn();
                    }
                }
            }
        }
    }
}

/// 拓扑排序: depends_on 的排在后面
fn topological_sort(processes: &BTreeMap<String, config::ProcessConfig>) -> Vec<String> {
    let mut ordered: Vec<String> = Vec::new();
    let mut remaining: BTreeMap<String, Option<String>> = processes
        .iter()
        .map(|(k, v)| (k.clone(), v.depends_on.clone()))
        .collect();

    while !remaining.is_empty() {
        let ready: Vec<String> = remaining
            .iter()
            .filter(|(_, dep)| {
                // 无依赖 或 依赖已在 ordered 中
                dep.as_ref().is_none_or(|d| ordered.contains(d))
            })
            .map(|(k, _)| k.clone())
            .collect();

        if ready.is_empty() {
            eprintln!("[guardian] 警告: 检测到循环依赖");
            // 按字母序加入剩余
            for k in remaining.keys() {
                ordered.push(k.clone());
            }
            break;
        }

        for r in ready {
            remaining.remove(&r);
            ordered.push(r);
        }
    }

    ordered
}

/// 等待进程变为 Running 状态
async fn wait_for_running(proc: &GuardedProcess, settings: &config::GuardianSettings) {
    let is_master = proc.name == "master";
    let timeout = settings.probe_timeout_secs;

    for attempt in 0..30 {
        if let Some(pid) = proc.pid {
            if !std::path::Path::new(&format!("/proc/{}", pid)).exists() {
                eprintln!("[guardian] {} PID={} 已退出", proc.name, pid);
                return;
            }
        }

        let health_addr = &proc.config.health_grpc;
        if health_addr.is_empty() {
            // 无健康检查地址，等 1s 直接认为就绪
            tokio::time::sleep(Duration::from_secs(1)).await;
            return;
        }

        let result = if is_master {
            probe_master(health_addr, timeout).await
        } else {
            probe_worker(health_addr, timeout).await
        };

        if result == ProbeResult::Running {
            eprintln!("[guardian] {} 健康检查通过", proc.name);
            return;
        }

        if attempt % 5 == 0 {
            eprintln!(
                "[guardian] 等待 {} 健康... (尝试 {}/30)",
                proc.name,
                attempt + 1
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
