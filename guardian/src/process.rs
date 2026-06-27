use crate::config::ProcessConfig;
use std::process::{Child, Command, Stdio};
use std::time::Instant;

/// 进程状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    Starting,
    Running,
    Degraded,
    Dead,
    Cooldown,
}

impl ProcessState {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            ProcessState::Starting => "starting",
            ProcessState::Running => "running",
            ProcessState::Degraded => "degraded",
            ProcessState::Dead => "dead",
            ProcessState::Cooldown => "cooldown",
        }
    }
}

/// 被守护的进程
pub struct GuardedProcess {
    pub name: String,
    pub config: ProcessConfig,
    pub pid: Option<u32>,
    pub state: ProcessState,
    pub child: Option<Child>,
    pub failures: u32,
    pub last_failure: Option<Instant>,
    pub cooldown_until: Option<Instant>,
}

impl GuardedProcess {
    pub fn new(name: String, config: ProcessConfig) -> Self {
        Self {
            name,
            config,
            pid: None,
            state: ProcessState::Starting,
            child: None,
            failures: 0,
            last_failure: None,
            cooldown_until: None,
        }
    }

    /// fork+exec 启动进程
    pub fn spawn(&mut self) -> anyhow::Result<()> {
        let mut cmd = Command::new(&self.config.path);
        cmd.args(&self.config.args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .stdin(Stdio::null());

        for (k, v) in &self.config.env {
            cmd.env(k, v);
        }

        let child = cmd.spawn()?;
        let pid = child.id();
        self.pid = Some(pid);
        self.child = Some(child);
        self.state = ProcessState::Starting;
        self.failures = 0;

        eprintln!("[guardian] {} 已启动, PID={}", self.name, pid);
        Ok(())
    }

    /// 强制终止进程
    pub fn kill(&mut self) {
        if let Some(ref mut child) = self.child {
            let pid = child.id();
            let _ = child.kill();
            let _ = child.wait();
            eprintln!("[guardian] {} (PID={}) 已强制终止", self.name, pid);
        }
        self.pid = None;
        self.child = None;
    }

    /// 检查被守护子进程是否仍在运行
    pub fn is_pid_alive(&mut self) -> bool {
        let Some(child) = self.child.as_mut() else {
            return false;
        };

        match child.try_wait() {
            Ok(None) => true,
            Ok(Some(_)) | Err(_) => {
                self.pid = None;
                self.child = None;
                false
            }
        }
    }

    /// 检查是否处于冷却期
    pub fn is_in_cooldown(&self) -> bool {
        if let Some(until) = self.cooldown_until {
            Instant::now() < until
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProcessConfig;

    fn test_config() -> ProcessConfig {
        #[cfg(windows)]
        let (path, args) = (
            "cmd".to_string(),
            vec!["/C".to_string(), "ping -n 6 127.0.0.1 > nul".to_string()],
        );

        #[cfg(not(windows))]
        let (path, args) = ("sleep".to_string(), vec!["5".to_string()]);

        ProcessConfig {
            path,
            args,
            env: Default::default(),
            health_grpc: String::new(),
            depends_on: None,
        }
    }

    #[test]
    fn test_spawn_and_kill() {
        let mut proc = GuardedProcess::new("test".to_string(), test_config());
        proc.spawn().unwrap();
        assert!(proc.pid.is_some());
        assert!(proc.is_pid_alive());
        assert_eq!(proc.state, ProcessState::Starting);

        proc.kill();
        assert!(proc.pid.is_none());
        assert!(!proc.is_pid_alive());
    }

    #[test]
    fn test_state_transitions() {
        let mut proc = GuardedProcess::new("test".to_string(), test_config());
        assert_eq!(proc.state, ProcessState::Starting);

        proc.spawn().unwrap();
        assert_eq!(proc.state, ProcessState::Starting);
        proc.failures = 0;

        proc.state = ProcessState::Running;
        proc.failures = 1;
        assert_eq!(proc.state, ProcessState::Running);

        proc.failures = 3;
        proc.state = ProcessState::Dead;
        assert_eq!(proc.state, ProcessState::Dead);
    }

    #[test]
    fn test_cooldown() {
        let mut proc = GuardedProcess::new("test".to_string(), test_config());
        assert!(!proc.is_in_cooldown());

        proc.cooldown_until = Some(Instant::now() + std::time::Duration::from_secs(300));
        assert!(proc.is_in_cooldown());

        proc.cooldown_until = Some(Instant::now() - std::time::Duration::from_secs(1));
        assert!(!proc.is_in_cooldown());
    }
}
