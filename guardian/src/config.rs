use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Guardian 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianConfig {
    #[serde(default)]
    pub guardian: GuardianSettings,
    #[serde(default)]
    pub processes: BTreeMap<String, ProcessConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardianSettings {
    #[serde(default = "default_probe_interval")]
    pub probe_interval_secs: u64,
    #[serde(default = "default_probe_timeout")]
    pub probe_timeout_secs: u64,
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_backoff_base")]
    pub backoff_base_secs: u64,
    #[serde(default = "default_backoff_max")]
    pub backoff_max_secs: u64,
    #[serde(default = "default_cooldown_after")]
    pub cooldown_after_failures: u32,
    #[serde(default = "default_cooldown_secs")]
    pub cooldown_secs: u64,
}

impl Default for GuardianSettings {
    fn default() -> Self {
        Self {
            probe_interval_secs: default_probe_interval(),
            probe_timeout_secs: default_probe_timeout(),
            failure_threshold: default_failure_threshold(),
            backoff_base_secs: default_backoff_base(),
            backoff_max_secs: default_backoff_max(),
            cooldown_after_failures: default_cooldown_after(),
            cooldown_secs: default_cooldown_secs(),
        }
    }
}

fn default_probe_interval() -> u64 { 5 }
fn default_probe_timeout() -> u64 { 3 }
fn default_failure_threshold() -> u32 { 3 }
fn default_backoff_base() -> u64 { 1 }
fn default_backoff_max() -> u64 { 60 }
fn default_cooldown_after() -> u32 { 10 }
fn default_cooldown_secs() -> u64 { 300 }

/// 单个进程配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessConfig {
    pub path: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// gRPC 健康检查地址
    #[serde(default)]
    pub health_grpc: String,
    /// 依赖的进程名（如 master）
    #[serde(default)]
    pub depends_on: Option<String>,
}

impl GuardianConfig {
    /// 从 YAML 文件加载配置
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let yaml = r#"
guardian:
  probe_interval_secs: 5
processes:
  master:
    path: "./bin/store_system"
    args: ["--config", "master.yaml"]
    health_grpc: "127.0.0.1:50051"
"#;
        let config: GuardianConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.guardian.probe_interval_secs, 5);
        assert_eq!(config.processes.len(), 1);
        assert_eq!(config.processes["master"].path, "./bin/store_system");
    }

    #[test]
    fn test_default_values() {
        let yaml = r#"
processes:
  w0:
    path: "./bin/store_system"
"#;
        let config: GuardianConfig = serde_yaml::from_str(yaml).unwrap();
        let g = &config.guardian;
        assert_eq!(g.probe_interval_secs, 5);
        assert_eq!(g.failure_threshold, 3);
        assert_eq!(g.backoff_max_secs, 60);
        assert_eq!(g.cooldown_secs, 300);
    }
}
