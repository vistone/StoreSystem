use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::{Result, StoreError};

/// 全局配置：从 YAML 文件加载所有可配置项
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// 运行模式: master | worker | standalone
    #[serde(default = "default_mode")]
    pub mode: String,

    /// 全局设置
    #[serde(default)]
    pub global: GlobalConfig,

    /// Master 节点配置
    #[serde(default)]
    pub master: MasterConfig,

    /// Worker 节点配置
    #[serde(default)]
    pub worker: WorkerConfig,

    /// 单机模式配置
    #[serde(default)]
    pub standalone: StandaloneConfig,

    /// 分片配置
    #[serde(default)]
    pub shard: ShardConfig,

    /// QuadKey 分片配置
    #[serde(default)]
    pub quad_shard: QuadShardConfig,

    /// Worker 默认配置（Master 持有，注册时下发给 Worker）
    #[serde(default)]
    pub worker_defaults: WorkerDefaultsConfig,

    /// Worker 区域分配映射（worker_id → region），由 Master 统一管理
    #[serde(default)]
    pub worker_regions: std::collections::HashMap<String, String>,
}

fn default_mode() -> String {
    "master".to_string()
}

// ============================================================
// 全局设置
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    /// 最大消息大小（字节），默认 256MB
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// Master 与 Worker 之间的通讯协议: "grpc" | "restful" | "ws" | "both"
    /// - "grpc": 仅使用 gRPC
    /// - "restful": 仅使用 RESTful HTTP
    /// - "ws": 仅使用 WebSocket
    /// - "both": 同时启动 gRPC 和 RESTful（默认）
    #[serde(default = "default_protocol")]
    pub protocol: String,
}

fn default_max_message_size() -> usize {
    256 * 1024 * 1024
}

fn default_protocol() -> String {
    "both".to_string()
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            max_message_size: default_max_message_size(),
            protocol: default_protocol(),
        }
    }
}

// ============================================================
// Master 节点配置
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterConfig {
    /// Master 监听地址
    #[serde(default = "default_master_listen")]
    pub listen_addr: String,

    /// Meta 数据库路径
    #[serde(default = "default_master_meta_path")]
    pub meta_path: String,

    /// Worker 心跳超时（秒）
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,

    /// 清理宕机 Worker 的间隔（秒）
    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_secs: u64,

    /// KV 数据库文件名（不含扩展名），统一下发给 Worker
    #[serde(default = "default_kv_name")]
    pub kv_name: String,

    /// KV 数据库文件扩展名
    #[serde(default = "default_kv_ext")]
    pub kv_ext: String,

    /// Meta 数据库文件名（不含扩展名）
    #[serde(default = "default_meta_name")]
    pub meta_name: String,

    /// Meta 数据库文件扩展名
    #[serde(default = "default_meta_ext")]
    pub meta_ext: String,

    /// 缓存大小，统一下发给 Worker
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,

    /// 刷盘间隔（毫秒），统一下发给 Worker
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,

    /// Pending 缓存配置（区域 Worker 宕机时 Master 本地兜底）
    #[serde(default)]
    pub pending: PendingConfig,
}

fn default_master_listen() -> String {
    "0.0.0.0:50051".to_string()
}

fn default_master_meta_path() -> String {
    "master_data/master.db".to_string()
}

fn default_heartbeat_timeout() -> u64 {
    30
}

fn default_cleanup_interval() -> u64 {
    60
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_master_listen(),
            meta_path: default_master_meta_path(),
            heartbeat_timeout_secs: default_heartbeat_timeout(),
            cleanup_interval_secs: default_cleanup_interval(),
            kv_name: default_kv_name(),
            kv_ext: default_kv_ext(),
            meta_name: default_meta_name(),
            meta_ext: default_meta_ext(),
            cache_size: default_cache_size(),
            flush_interval_ms: default_flush_interval(),
            pending: PendingConfig::default(),
        }
    }
}

// ============================================================
// Pending 缓存配置
// ============================================================

/// Master 本地 Pending 缓存配置
/// 当区域 Worker 不可用时，Master 接管写入并存入本地持久化缓存。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingConfig {
    /// 数据目录（默认 master_data/pending）
    #[serde(default = "default_pending_data_dir")]
    pub data_dir: String,
    /// 惰性 GC 间隔（秒），默认 60
    #[serde(default = "default_pending_gc_interval")]
    pub gc_interval_secs: u64,
    /// flushing 超时（秒），超过后回退为 pending，默认 60
    #[serde(default = "default_pending_flush_timeout")]
    pub flush_timeout_secs: u64,
}

fn default_pending_data_dir() -> String {
    "master_data/pending".to_string()
}

fn default_pending_gc_interval() -> u64 {
    60
}

fn default_pending_flush_timeout() -> u64 {
    60
}

impl Default for PendingConfig {
    fn default() -> Self {
        Self {
            data_dir: default_pending_data_dir(),
            gc_interval_secs: default_pending_gc_interval(),
            flush_timeout_secs: default_pending_flush_timeout(),
        }
    }
}

// ============================================================
// Worker 默认配置（Master 下发给所有 Worker）
// ============================================================

/// Worker 默认配置（Master 持有，注册时下发给 Worker）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerDefaultsConfig {
    /// KV 数据库文件扩展名
    #[serde(default = "default_kv_ext")]
    pub kv_ext: String,

    /// Meta 数据库文件扩展名
    #[serde(default = "default_meta_ext")]
    pub meta_ext: String,

    /// 缓存大小
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,

    /// 刷盘间隔（毫秒）
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,

    /// 心跳间隔（秒）
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,

    /// Worker 权重
    #[serde(default = "default_weight")]
    pub weight: i32,

    /// QuadKey 分片配置
    #[serde(default)]
    pub quad_shard: QuadShardConfig,
}

impl Default for WorkerDefaultsConfig {
    fn default() -> Self {
        Self {
            kv_ext: default_kv_ext(),
            meta_ext: default_meta_ext(),
            cache_size: default_cache_size(),
            flush_interval_ms: default_flush_interval(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            weight: default_weight(),
            quad_shard: QuadShardConfig::default(),
        }
    }
}

// ============================================================
// Worker 节点配置
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Worker 唯一标识
    #[serde(default = "default_worker_id")]
    pub worker_id: String,

    /// Worker 监听地址（gRPC 服务）
    #[serde(default = "default_worker_listen")]
    pub listen_addr: String,

    /// Master 地址（用于注册和心跳）
    #[serde(default = "default_master_addr")]
    pub master_addr: String,

    /// Worker 数据目录
    #[serde(default = "default_worker_data_dir")]
    pub data_dir: String,

    /// KV 数据库文件名（不含扩展名）
    #[serde(default = "default_kv_name")]
    pub kv_name: String,

    /// KV 数据库文件扩展名
    #[serde(default = "default_kv_ext")]
    pub kv_ext: String,

    /// Meta 数据库文件名（不含扩展名）
    #[serde(default = "default_meta_name")]
    pub meta_name: String,

    /// Meta 数据库文件扩展名
    #[serde(default = "default_meta_ext")]
    pub meta_ext: String,

    /// 缓存大小
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,

    /// 刷盘间隔（毫秒）
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,

    /// 心跳间隔（秒）
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,

    /// Worker 权重
    #[serde(default = "default_weight")]
    pub weight: i32,

    /// Worker 负责的 quadkey 区域 (0/1/2/3)
    #[serde(default = "default_region")]
    pub region: String,

    /// Master WebSocket 地址（用于日志推送）
    #[serde(default = "default_master_ws_addr")]
    pub master_ws_addr: String,
}

fn default_worker_id() -> String {
    "worker-1".to_string()
}

fn default_worker_listen() -> String {
    "0.0.0.0:50061".to_string()
}

fn default_master_addr() -> String {
    "http://127.0.0.1:50051".to_string()
}

fn default_worker_data_dir() -> String {
    "worker_data/worker-1".to_string()
}

fn default_kv_name() -> String {
    "kv".to_string()
}

fn default_kv_ext() -> String {
    ".db".to_string()
}

fn default_meta_name() -> String {
    "meta".to_string()
}

fn default_meta_ext() -> String {
    ".db".to_string()
}

fn default_cache_size() -> usize {
    10000
}

fn default_flush_interval() -> u64 {
    5
}

fn default_heartbeat_interval() -> u64 {
    10
}

fn default_weight() -> i32 {
    1
}

fn default_region() -> String {
    "0".to_string()
}

fn default_master_ws_addr() -> String {
    "127.0.0.1:50053".to_string()
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: default_worker_id(),
            listen_addr: default_worker_listen(),
            master_addr: default_master_addr(),
            data_dir: default_worker_data_dir(),
            kv_name: default_kv_name(),
            kv_ext: default_kv_ext(),
            meta_name: default_meta_name(),
            meta_ext: default_meta_ext(),
            cache_size: default_cache_size(),
            flush_interval_ms: default_flush_interval(),
            heartbeat_interval_secs: default_heartbeat_interval(),
            weight: default_weight(),
            region: default_region(),
            master_ws_addr: default_master_ws_addr(),
        }
    }
}

impl WorkerConfig {
    /// 获取 KV 数据库完整路径
    pub fn kv_path(&self) -> PathBuf {
        let dir = Path::new(&self.data_dir);
        dir.join(format!("{}{}", self.kv_name, self.kv_ext))
    }

    /// 获取 Meta 数据库完整路径
    pub fn meta_path(&self) -> PathBuf {
        let dir = Path::new(&self.data_dir);
        dir.join(format!("{}{}", self.meta_name, self.meta_ext))
    }
}

// ============================================================
// 单机模式配置
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StandaloneConfig {
    /// KV 数据库路径
    #[serde(default = "default_standalone_kv_path")]
    pub kv_path: String,

    /// Meta 数据库路径
    #[serde(default = "default_standalone_meta_path")]
    pub meta_path: String,

    /// 缓存大小
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,

    /// 刷盘间隔（毫秒）
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,

    /// RESTful HTTP 服务端口
    #[serde(default = "default_http_port")]
    pub http_port: u16,

    /// gRPC 服务端口
    #[serde(default = "default_grpc_port")]
    pub grpc_port: u16,
}

fn default_standalone_kv_path() -> String {
    "data/kv.db".to_string()
}

fn default_standalone_meta_path() -> String {
    "data/meta.db".to_string()
}

fn default_http_port() -> u16 {
    8080
}

fn default_grpc_port() -> u16 {
    50051
}

impl Default for StandaloneConfig {
    fn default() -> Self {
        Self {
            kv_path: default_standalone_kv_path(),
            meta_path: default_standalone_meta_path(),
            cache_size: default_cache_size(),
            flush_interval_ms: default_flush_interval(),
            http_port: default_http_port(),
            grpc_port: default_grpc_port(),
        }
    }
}

// ============================================================
// 分片配置
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    /// 分片数量
    #[serde(default = "default_num_shards")]
    pub num_shards: usize,

    /// 数据目录
    #[serde(default = "default_shard_data_dir")]
    pub data_dir: String,

    /// KV 数据库文件名模板
    #[serde(default = "default_kv_name_template")]
    pub kv_name_template: String,

    /// KV 数据库文件扩展名
    #[serde(default = "default_kv_ext")]
    pub kv_ext: String,

    /// Meta 数据库文件名模板
    #[serde(default = "default_meta_name_template")]
    pub meta_name_template: String,

    /// Meta 数据库文件扩展名
    #[serde(default = "default_meta_ext")]
    pub meta_ext: String,

    /// 缓存大小（每个分片）
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,

    /// 刷盘间隔（毫秒）
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
}

fn default_num_shards() -> usize {
    4
}

fn default_shard_data_dir() -> String {
    "shard_data".to_string()
}

fn default_kv_name_template() -> String {
    "kv_{}".to_string()
}

fn default_meta_name_template() -> String {
    "meta_{}".to_string()
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            num_shards: default_num_shards(),
            data_dir: default_shard_data_dir(),
            kv_name_template: default_kv_name_template(),
            kv_ext: default_kv_ext(),
            meta_name_template: default_meta_name_template(),
            meta_ext: default_meta_ext(),
            cache_size: default_cache_size(),
            flush_interval_ms: default_flush_interval(),
        }
    }
}

// ============================================================
// QuadKey 分片配置
// ============================================================

/// QuadKey 分片配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuadShardConfig {
    /// 层级 ≤ base_level 时，所有数据存入 base DB
    #[serde(default = "default_base_level")]
    pub base_level: u32,
    /// 层级阈值：base < level < split_level 用 4 位前缀，≥ split_level 用 8 位
    #[serde(default = "default_split_level")]
    pub split_level: u32,
    /// 数据根目录
    #[serde(default = "default_quad_data_dir")]
    pub data_dir: String,
    /// KV 数据库扩展名
    #[serde(default = "default_kv_ext")]
    pub kv_ext: String,
    /// Meta 数据库扩展名
    #[serde(default = "default_meta_ext")]
    pub meta_ext: String,
    /// 缓存大小
    #[serde(default = "default_cache_size")]
    pub cache_size: usize,
    /// 刷盘间隔（毫秒）
    #[serde(default = "default_flush_interval")]
    pub flush_interval_ms: u64,
}

fn default_base_level() -> u32 {
    8
}
fn default_split_level() -> u32 {
    18
}
fn default_quad_data_dir() -> String {
    "quad_data".to_string()
}

impl Default for QuadShardConfig {
    fn default() -> Self {
        Self {
            base_level: default_base_level(),
            split_level: default_split_level(),
            data_dir: default_quad_data_dir(),
            kv_ext: default_kv_ext(),
            meta_ext: default_meta_ext(),
            cache_size: default_cache_size(),
            flush_interval_ms: default_flush_interval(),
        }
    }
}

// ============================================================
// 配置加载
// ============================================================

impl AppConfig {
    /// 从 YAML 文件加载配置（失败时返回错误而不是静默回退默认值）
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let content = std::fs::read_to_string(path).map_err(|e| {
            StoreError::InvalidArgument(format!("读取配置文件 '{}' 失败: {}", path.display(), e))
        })?;
        let config = serde_yaml::from_str(&content).map_err(|e| {
            StoreError::InvalidArgument(format!("解析配置文件 '{}' 失败: {}", path.display(), e))
        })?;
        println!("[Config] 已加载配置文件: {}", path.display());
        Ok(config)
    }
}

impl std::str::FromStr for AppConfig {
    type Err = serde_yaml::Error;

    /// Parse an `AppConfig` from a YAML string.
    fn from_str(yaml: &str) -> std::result::Result<Self, Self::Err> {
        serde_yaml::from_str(yaml)
    }
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            global: GlobalConfig::default(),
            master: MasterConfig::default(),
            worker: WorkerConfig::default(),
            standalone: StandaloneConfig::default(),
            shard: ShardConfig::default(),
            quad_shard: QuadShardConfig::default(),
            worker_defaults: WorkerDefaultsConfig::default(),
            worker_regions: std::collections::HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_defaults_deserialize() {
        let yaml = r#"
worker_defaults:
  kv_ext: ".g3db"
  meta_ext: ".bulk"
  cache_size: 10000
  flush_interval_ms: 5
  heartbeat_interval_secs: 10
  weight: 1
  quad_shard:
    base_level: 8
    split_level: 18
    data_dir: "quad_data"
    kv_ext: ".kv"
    meta_ext: ".db"
    cache_size: 10000
    flush_interval_ms: 5
"#;
        let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.worker_defaults.kv_ext, ".g3db");
        assert_eq!(config.worker_defaults.meta_ext, ".bulk");
        assert_eq!(config.worker_defaults.cache_size, 10000);
        assert_eq!(config.worker_defaults.quad_shard.base_level, 8);
        assert_eq!(config.worker_defaults.quad_shard.split_level, 18);
        assert_eq!(config.worker_defaults.quad_shard.kv_ext, ".kv");
    }

    #[test]
    fn test_worker_regions_deserialize() {
        let yaml = r#"
worker_regions:
  worker-0: "0"
  worker-1: "1"
  worker-2: "2"
  worker-3: "3"
"#;
        let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.worker_regions.get("worker-0"), Some(&"0".to_string()));
        assert_eq!(config.worker_regions.get("worker-1"), Some(&"1".to_string()));
        assert_eq!(config.worker_regions.get("worker-3"), Some(&"3".to_string()));
    }

    #[test]
    fn test_worker_defaults_default_values() {
        let wd = WorkerDefaultsConfig::default();
        assert_eq!(wd.kv_ext, ".db");
        assert_eq!(wd.cache_size, 10000);
        assert_eq!(wd.flush_interval_ms, 5);
        assert_eq!(wd.heartbeat_interval_secs, 10);
        assert_eq!(wd.weight, 1);
        assert_eq!(wd.quad_shard.base_level, 8);
    }
}
