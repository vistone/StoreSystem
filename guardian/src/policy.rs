use crate::config::GuardianSettings;
use crate::process::{GuardedProcess, ProcessState};
use std::time::{Duration, Instant};

/// 根据退避策略计算重启等待时间
pub fn backoff_delay(failures: u32, base_secs: u64, max_secs: u64) -> Duration {
    let delay = base_secs.saturating_mul(2u64.saturating_pow(failures.saturating_sub(1)));
    Duration::from_secs(delay.min(max_secs))
}

/// 处理 Dead 状态进程的重启决策
///
/// 返回 Some(delay) 表示等待 delay 后重启，None 表示不重启（冷却中）
pub fn handle_dead(proc: &mut GuardedProcess, settings: &GuardianSettings) -> Option<Duration> {
    // 1. 冷却期检查
    if proc.is_in_cooldown() {
        let remaining = proc.cooldown_until.unwrap().duration_since(Instant::now());
        proc.state = ProcessState::Cooldown;
        eprintln!(
            "[guardian] {} 处于冷却期，剩余 {}s",
            proc.name,
            remaining.as_secs()
        );
        return None;
    }

    // 2. 超过冷却阈值 → 进入冷却
    if proc.failures >= settings.cooldown_after_failures {
        let cooldown = Duration::from_secs(settings.cooldown_secs);
        proc.cooldown_until = Some(Instant::now() + cooldown);
        proc.state = ProcessState::Cooldown;
        eprintln!(
            "[guardian] {} 连续失败 {} 次，进入冷却 {}s",
            proc.name, proc.failures, settings.cooldown_secs
        );
        return None;
    }

    // 3. kill 旧进程
    proc.kill();

    // 4. 退避等待
    let delay = backoff_delay(proc.failures, settings.backoff_base_secs, settings.backoff_max_secs);
    eprintln!(
        "[guardian] {} 退避等待 {}s 后重启 (failures={})",
        proc.name,
        delay.as_secs(),
        proc.failures
    );

    Some(delay)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_settings() -> GuardianSettings {
        GuardianSettings {
            probe_interval_secs: 5,
            probe_timeout_secs: 3,
            failure_threshold: 3,
            backoff_base_secs: 1,
            backoff_max_secs: 60,
            cooldown_after_failures: 10,
            cooldown_secs: 300,
        }
    }

    #[test]
    fn test_backoff_delay() {
        assert_eq!(backoff_delay(1, 1, 60), Duration::from_secs(1));
        assert_eq!(backoff_delay(3, 1, 60), Duration::from_secs(4)); // 1*2^2=4
        assert_eq!(backoff_delay(5, 1, 60), Duration::from_secs(16)); // 1*2^4=16
        assert_eq!(backoff_delay(10, 1, 60), Duration::from_secs(60)); // capped
    }

    #[test]
    fn test_handle_dead_restart() {
        let mut proc = GuardedProcess::new(
            "test".to_string(),
            crate::config::ProcessConfig {
                path: "true".to_string(),
                args: vec![],
                env: Default::default(),
                health_grpc: String::new(),
                depends_on: None,
            },
        );
        proc.failures = 3;
        proc.state = ProcessState::Dead;

        let result = handle_dead(&mut proc, &test_settings());
        assert!(result.is_some()); // 应该重启
        assert!(result.unwrap().as_secs() >= 1);
    }

    #[test]
    fn test_handle_dead_cooldown() {
        let mut proc = GuardedProcess::new(
            "test".to_string(),
            crate::config::ProcessConfig {
                path: "true".to_string(),
                args: vec![],
                env: Default::default(),
                health_grpc: String::new(),
                depends_on: None,
            },
        );
        proc.failures = 15;
        proc.state = ProcessState::Dead;

        let result = handle_dead(&mut proc, &test_settings());
        assert!(result.is_none()); // 冷却中
        assert_eq!(proc.state, ProcessState::Cooldown);
        assert!(proc.cooldown_until.is_some());
    }
}
