# Master 统一配置管理架构 实现计划

**目标：** 将配置管理权从 Worker 本地 yaml 收归 Master 统一管理，Master 通过 gRPC 注册响应下发初始配置，通过日志 WebSocket 连接反向推送配置变更。

**架构：** Master 持有 `worker_defaults` + `worker_regions` 配置块；Worker 启动时只读极简 yaml（4 字段），注册时 Master 返回完整 `WorkerConfig`；Master 复用现有日志 WS 连接（端口 50053）反向推送 `config_update` 消息，Worker 识别后热更新性能参数。

**技术栈：** Rust + tonic + tokio-tungstenite + serde_yaml

## 全局约束

- 禁止重写现有代码，改动一律用 `edit_file`，最小 diff
- 使用 `anyhow`/`thiserror` 进行错误处理，禁止裸 `unwrap()`
- 公共 API 必须有文档注释
- 编译: `cargo build --release` 必须 exit 0
- 单元测试: `cargo test` 必须全部通过
- 集成测试: `make test-fault` 必须全部通过
- 每个任务完成后提交，commit message 用 `feat:` / `refactor:` 前缀
- proto 新增字段有默认值，保持向后兼容

---

### 任务 1: Proto 协议扩展

**文件：**
- 修改: `proto/store.proto:134-148`（扩展 RegisterWorkerResponse）
- 修改: `proto/store.proto`（新增 WorkerConfig / QuadShardConfig 消息）

- [ ] **步骤 1: 扩展 RegisterWorkerResponse**

  在 `proto/store.proto` 的 `RegisterWorkerResponse` 消息中新增两个字段：
  ```proto
  message RegisterWorkerResponse {
    bool success = 1;
    string message = 2;
    WorkerConfig config = 3;       // 新增：Master 下发的完整 Worker 配置
    uint64 config_version = 4;     // 新增：配置版本号
  }
  ```

- [ ] **步骤 2: 新增 WorkerConfig 消息**

  在 `RegisterWorkerResponse` 之后新增：
  ```proto
  // Master 下发给 Worker 的运行配置
  message WorkerConfig {
    string region = 1;
    string kv_ext = 2;
    string meta_ext = 3;
    uint64 cache_size = 4;
    uint64 flush_interval_ms = 5;
    uint64 heartbeat_interval_secs = 6;
    int32 weight = 7;
    QuadShardConfig quad_shard = 8;
  }

  message QuadShardConfig {
    uint32 base_level = 1;
    uint32 split_level = 2;
    string data_dir = 3;
    string kv_ext = 4;
    string meta_ext = 5;
    uint64 cache_size = 6;
    uint64 flush_interval_ms = 7;
  }
  ```

- [ ] **步骤 3: 验证编译**

  `cargo build`
  预期: PASS（proto 新字段有默认值，不影响现有代码）

- [ ] **步骤 4: 提交**

  `git commit -m "feat(proto): 扩展 RegisterWorkerResponse 下发 WorkerConfig"`

---

### 任务 2: Config 模块重构

**文件：**
- 修改: `src/config.rs`（新增 WorkerDefaultsConfig，AppConfig 增加 worker_defaults / worker_regions 字段）

- [ ] **步骤 1: 编写失败测试**

  在 `src/config.rs` 末尾新增测试模块：
  ```rust
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
          assert_eq!(config.worker_defaults.quad_shard.base_level, 8);
      }

      #[test]
      fn test_worker_regions_deserialize() {
          let yaml = r#"
  worker_regions:
    worker-0: "0"
    worker-1: "1"
  "#;
          let config: AppConfig = serde_yaml::from_str(yaml).unwrap();
          assert_eq!(config.worker_regions.get("worker-0"), Some(&"0".to_string()));
      }
  }
  ```
  `cargo test test_worker_defaults_deserialize`
  预期: FAIL（字段不存在）

- [ ] **步骤 2: 新增 WorkerDefaultsConfig 结构**

  在 `src/config.rs` 中 `MasterConfig` 之后新增：
  ```rust
  /// Worker 默认配置（Master 下发给所有 Worker）
  #[derive(Debug, Clone, Serialize, Deserialize)]
  pub struct WorkerDefaultsConfig {
      #[serde(default = "default_kv_ext")]
      pub kv_ext: String,
      #[serde(default = "default_meta_ext")]
      pub meta_ext: String,
      #[serde(default = "default_cache_size")]
      pub cache_size: usize,
      #[serde(default = "default_flush_interval")]
      pub flush_interval_ms: u64,
      #[serde(default = "default_heartbeat_interval")]
      pub heartbeat_interval_secs: u64,
      #[serde(default = "default_weight")]
      pub weight: i32,
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
  ```

- [ ] **步骤 3: AppConfig 增加字段**

  在 `AppConfig` 中新增：
  ```rust
  /// Worker 默认配置（Master 下发）
  #[serde(default)]
  pub worker_defaults: WorkerDefaultsConfig,

  /// Worker 区域分配映射（worker_id → region）
  #[serde(default)]
  pub worker_regions: std::collections::HashMap<String, String>,
  ```

  在 `AppConfig::default()` 中初始化这两个字段。

- [ ] **步骤 4: 验证测试通过**

  `cargo test test_worker_defaults_deserialize test_worker_regions_deserialize`
  预期: PASS

- [ ] **步骤 5: 验证全量编译**

  `cargo build`
  预期: PASS

- [ ] **步骤 6: 提交**

  `git commit -m "feat(config): 新增 WorkerDefaultsConfig 和 worker_regions 配置"`

---

### 任务 3: MasterNode 持有配置并返回

**文件：**
- 修改: `src/master.rs:108-130`（MasterNode 增加 worker_defaults / worker_regions / config_version 字段）
- 修改: `src/master.rs:193-240`（register_worker 返回完整配置）
- 修改: `src/main.rs:67-90`（run_master 传入新配置）

- [ ] **步骤 1: 编写失败测试**

  在 `src/master.rs` 末尾新增测试：
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use crate::config::{WorkerDefaultsConfig, QuadShardConfig};
      use std::collections::HashMap;

      #[tokio::test]
      async fn test_register_returns_config() {
          // 准备临时 master
          let tmp = tempfile::NamedTempFile::new().unwrap();
          let mut master_config = MasterConfig::default();
          master_config.meta_path = tmp.path().to_string_lossy().to_string();

          let mut worker_defaults = WorkerDefaultsConfig::default();
          worker_defaults.kv_ext = ".g3db".to_string();
          let mut regions = HashMap::new();
          regions.insert("worker-0".to_string(), "0".to_string());

          let master = MasterNode::open_with_worker_defaults(
              master_config,
              "both",
              worker_defaults,
              regions,
          ).unwrap();

          // 注册 worker（region 由 Master 分配，传入空字符串）
          let config = master.register_worker("worker-0", "0.0.0.0:50061", 1, HashMap::new(), "").await.unwrap();

          // 验证返回的配置
          assert_eq!(config.region, "0");
          assert_eq!(config.kv_ext, ".g3db");
          assert_eq!(config.config_version, 1);
      }
  }
  ```
  `cargo test test_register_returns_config`
  预期: FAIL（方法不存在）

- [ ] **步骤 2: MasterNode 增加字段**

  在 `MasterNode` 结构体中新增：
  ```rust
  /// Worker 默认配置（下发给所有 Worker）
  worker_defaults: Arc<crate::config::WorkerDefaultsConfig>,
  /// Worker 区域分配映射
  worker_regions: Arc<std::collections::HashMap<String, String>>,
  /// 配置版本号（每次变更递增）
  config_version: Arc<std::sync::atomic::AtomicU64>,
  ```

- [ ] **步骤 3: 新增 open_with_worker_defaults 方法**

  在 `impl MasterNode` 中新增（基于现有 `open_with_protocol` 扩展，不改动原方法）：
  ```rust
  pub fn open_with_worker_defaults(
      config: MasterConfig,
      protocol: &str,
      worker_defaults: crate::config::WorkerDefaultsConfig,
      worker_regions: std::collections::HashMap<String, String>,
  ) -> Result<Self> {
      // 复用 open_with_protocol 的逻辑，再注入新字段
      // ...（实现细节）
  }
  ```

- [ ] **步骤 4: 改造 register_worker 返回配置**

  修改 `register_worker` 方法签名，返回 `Result<WorkerConfigPayload>`：
  ```rust
  pub async fn register_worker(
      &self,
      worker_id: &str,
      address: &str,
      weight: i32,
      tags: HashMap<String, String>,
      _client_region: &str,  // 忽略客户端传入的 region
  ) -> Result<WorkerConfigPayload>
  ```

  其中 `WorkerConfigPayload` 是新增结构体，包含 region / kv_ext / cache_size / quad_shard / config_version。

  逻辑：
  1. 查 `worker_regions` 获取 region，找不到返回错误
  2. 用 `worker_defaults` 构建配置
  3. 写入 SQLite + 内存缓存（region 用查到的值）
  4. 返回配置 + config_version

- [ ] **步骤 5: 验证测试通过**

  `cargo test test_register_returns_config`
  预期: PASS

- [ ] **步骤 6: 验证全量编译**

  `cargo build`
  预期: PASS（可能有 warning，因为 MasterAdminService 还没改造，但不应有 error）

- [ ] **步骤 7: 提交**

  `git commit -m "feat(master): register_worker 返回完整 WorkerConfig"`

---

### 任务 4: MasterAdminService gRPC 适配

**文件：**
- 修改: `src/master.rs:1441-1470`（register_worker gRPC 方法返回 config）

- [ ] **步骤 1: 改造 register_worker gRPC 方法**

  修改 `MasterAdminService::register_worker`，调用新的 `register_worker` 返回值，填充 `RegisterWorkerResponse.config` 和 `config_version`：
  ```rust
  let payload = self.master
      .register_worker(&req.worker_id, &req.address, req.weight, req.tags, &req.region)
      .await
      .map_err(|e| Status::internal(e.to_string()))?;

  Ok(Response::new(proto::RegisterWorkerResponse {
      success: true,
      message: format!("Worker {} 注册成功", req.worker_id),
      config: Some(proto::WorkerConfig {
          region: payload.region,
          kv_ext: payload.kv_ext,
          // ... 其他字段
      }),
      config_version: payload.config_version,
  }))
  ```

- [ ] **步骤 2: 验证编译**

  `cargo build`
  预期: PASS

- [ ] **步骤 3: 提交**

  `git commit -m "feat(master): gRPC register_worker 返回 WorkerConfig"`

---

### 任务 5: Worker 注册流程改造

**文件：**
- 修改: `src/main.rs:142-280`（run_worker 改为注册后用返回的配置初始化）
- 修改: `src/main.rs:385-410`（register_with_master 返回配置）

- [ ] **步骤 1: 改造 register_with_master 函数**

  修改 `register_with_master` 返回 `Result<proto::WorkerConfig>`：
  ```rust
  async fn register_with_master(
      master_addr: &str,
      worker_id: &str,
      listen_addr: &str,
  ) -> Result<proto::WorkerConfig, Box<dyn std::error::Error>> {
      // ... 调用 gRPC，返回 response.config
  }
  ```
  移除 `region` 参数（Master 分配）。

- [ ] **步骤 2: 改造 run_worker 函数**

  修改 `run_worker` 流程：
  1. 读极简 worker.yaml（只含 worker_id / listen_addr / master_addr / data_dir）
  2. 调用 `register_with_master` 获取 `WorkerConfig`
  3. 用返回的 config 构建 `WorkerConfig`（Rust 结构体），初始化 `WorkerNode`
  4. 从 `master_addr` 推导 `master_ws_addr`（同主机 + :50053）
  5. 启动心跳（用 config 中的 heartbeat_interval_secs）
  6. 启动日志 WS 客户端（监听 config_update）

  关键代码骨架：
  ```rust
  let proto_config = register_with_master(&master_addr_http, &wc.worker_id, &wc.listen_addr).await?;
  println!("   ✅ 注册成功, 配置版本: {}", proto_config.config_version);

  // 用 Master 下发的配置构建 WorkerConfig
  let worker_config = WorkerConfig::new(
      &wc.worker_id,
      &wc.listen_addr,
      &wc.master_addr,
      &wc.data_dir,
  )
  .with_kv_path(format!("{}/kv{}", wc.data_dir, proto_config.kv_ext))
  .with_meta_path(format!("{}/meta{}", wc.data_dir, proto_config.meta_ext))
  .with_cache_size(proto_config.cache_size as usize)
  .with_flush_interval(proto_config.flush_interval_ms)
  .with_heartbeat_interval(proto_config.heartbeat_interval_secs)
  .with_weight(proto_config.weight)
  .with_quad_shard_config(/* 从 proto 转换 */);
  ```

- [ ] **步骤 3: 验证编译**

  `cargo build`
  预期: PASS

- [ ] **步骤 4: 提交**

  `git commit -m "refactor(worker): 注册后用 Master 下发的配置初始化"`

---

### 任务 6: Master 配置推送通道

**文件：**
- 修改: `src/master_log_ws.rs`（维护 worker_id → sender 映射，支持广播 config_update）

- [ ] **步骤 1: 编写失败测试**

  在 `src/master_log_ws.rs` 新增测试模块，验证 `ConfigBroadcaster` 能注册 worker 并广播：
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      use tokio::sync::mpsc;

      #[tokio::test]
      async fn test_config_broadcaster() {
          let broadcaster = ConfigBroadcaster::new();
          let (tx, mut rx) = mpsc::unbounded_channel::<Message>();
          broadcaster.register("worker-0".to_string(), tx).await;

          broadcaster.broadcast_config_update("worker-0", r#"{"type":"config_update"}"#).await;

          let msg = rx.recv().await.expect("should receive");
          assert!(msg.to_text().unwrap().contains("config_update"));
      }
  }
  ```
  `cargo test test_config_broadcaster`
  预期: FAIL（ConfigBroadcaster 不存在）

- [ ] **步骤 2: 新增 ConfigBroadcaster**

  在 `src/master_log_ws.rs` 中新增：
  ```rust
  use tokio::sync::mpsc;
  use dashmap::DashMap;

  /// 配置推送器：维护 worker_id → WS sender 的映射
  pub struct ConfigBroadcaster {
      senders: DashMap<String, mpsc::UnboundedSender<Message>>,
  }

  impl ConfigBroadcaster {
      pub fn new() -> Self {
          Self { senders: DashMap::new() }
      }

      pub async fn register(&self, worker_id: String, tx: mpsc::UnboundedSender<Message>) {
          self.senders.insert(worker_id, tx);
      }

      pub async fn unregister(&self, worker_id: &str) {
          self.senders.remove(worker_id);
      }

      pub async fn broadcast_config_update(&self, worker_id: &str, config_json: &str) {
          if let Some(tx) = self.senders.get(worker_id) {
              let _ = tx.send(Message::Text(config_json.to_string()));
          }
      }

      pub async fn broadcast_all(&self, config_json: &str) {
          for entry in self.senders.iter() {
              let _ = entry.value().send(Message::Text(config_json.to_string()));
          }
      }
  }
  ```

- [ ] **步骤 3: MasterLogWsServer 集成 ConfigBroadcaster**

  修改 `MasterLogWsServer`：
  1. 新增 `broadcaster: Arc<ConfigBroadcaster>` 字段
  2. `new()` 中初始化
  3. `handle_log_connection` 中：从第一条日志消息提取 worker_id，注册到 broadcaster
  4. 连接关闭时 unregister

- [ ] **步骤 4: MasterNode 持有 broadcaster 引用**

  修改 `MasterNode`，新增 `config_broadcaster: Arc<ConfigBroadcaster>` 字段，供后续配置变更时调用。

- [ ] **步骤 5: 验证测试通过**

  `cargo test test_config_broadcaster`
  预期: PASS

- [ ] **步骤 6: 验证全量编译**

  `cargo build`
  预期: PASS

- [ ] **步骤 7: 提交**

  `git commit -m "feat(master): ConfigBroadcaster 支持配置推送"`

---

### 任务 7: Worker 接收配置更新

**文件：**
- 修改: `src/logger.rs`（WorkerLogger 读取循环识别 config_update 消息）
- 修改: `src/worker.rs`（WorkerNode 新增 apply_config_update 方法）

- [ ] **步骤 1: 编写失败测试**

  在 `src/worker.rs` 新增测试：
  ```rust
  #[cfg(test)]
  mod config_update_tests {
      use super::*;
      use crate::config::QuadShardConfig;

      #[test]
      fn test_apply_config_update_hot_params() {
          // 构造 WorkerNode，应用配置更新，验证 cache_size 等热更新
      }

      #[test]
      fn test_apply_config_update_cold_params_warns() {
          // 构造 WorkerNode，应用 kv_ext 变更，验证不应用且打告警
      }
  }
  ```
  `cargo test config_update_tests`
  预期: FAIL（方法不存在）

- [ ] **步骤 2: WorkerNode 新增 apply_config_update**

  在 `src/worker.rs` 的 `impl WorkerNode` 中新增：
  ```rust
  /// 应用配置更新（热更新性能参数，冷参数打告警）
  pub fn apply_config_update(&self, new_config: &WorkerRuntimeConfig) {
      // 热更新：cache_size, flush_interval_ms, heartbeat_interval_secs, weight
      if self.config.cache_size != new_config.cache_size {
          println!("[Worker] 热更新 cache_size: {} -> {}", self.config.cache_size, new_config.cache_size);
          // 注意：实际 cache resize 需要在 KvStore 中实现，此处先记录
      }
      // 冷参数：kv_ext, meta_ext, quad_shard, region
      if self.config.kv_path != new_config.kv_path {
          eprintln!("[Worker] WARN 配置变更需重启生效: kv_path {} -> {}", self.config.kv_path, new_config.kv_path);
      }
      // ... 其他冷参数检查
  }
  ```

  新增 `WorkerRuntimeConfig` 结构体，包含可热更新和不可热更新的参数。

- [ ] **步骤 3: WorkerLogger 识别 config_update**

  修改 `src/logger.rs` 的 `WorkerLogger`，在 WS 读取循环中：
  1. 收到文本消息时，尝试解析为 `config_update` 类型
  2. 如果是 config_update，调用回调函数（通过 `on_config_update: Option<Arc<dyn Fn(WorkerRuntimeConfig) + Send + Sync>>`）
  3. 否则按原逻辑处理（日志响应）

- [ ] **步骤 4: main.rs 注册回调**

  在 `run_worker` 中，创建 WorkerLogger 后注册回调：
  ```rust
  let node_for_callback = node_arc.clone();
  worker_logger.on_config_update(move |config| {
      node_for_callback.apply_config_update(&config);
  });
  ```

- [ ] **步骤 5: 验证测试通过**

  `cargo test config_update_tests`
  预期: PASS

- [ ] **步骤 6: 验证全量编译**

  `cargo build`
  预期: PASS

- [ ] **步骤 7: 提交**

  `git commit -m "feat(worker): 接收并应用 config_update 热更新"`

---

### 任务 8: 配置文件变更

**文件：**
- 修改: `master.yaml`（新增 worker_defaults / worker_regions 段）
- 修改: `worker-0.yaml` ~ `worker-3.yaml`（精简为 4 字段）
- 删除: `config.yaml`

- [x] **步骤 1: 改造 master.yaml**

  在 `master.yaml` 中新增 `worker_defaults` 和 `worker_regions` 段（见设计文档第 4.1 节）。

- [x] **步骤 2: 精简 worker-0.yaml ~ worker-3.yaml**

  每个 worker-N.yaml 只保留：
  ```yaml
  mode: worker
  worker:
    worker_id: "worker-N"
    listen_addr: "0.0.0.0:5006{N+1}"
    master_addr: "http://127.0.0.1:50051"
    data_dir: "worker_data/worker-N"
  ```

- [x] **步骤 3: 删除 config.yaml**

  删除 `config.yaml` 文件。

- [x] **步骤 4: 验证编译**

  `cargo build`
  预期: PASS

- [ ] **步骤 5: 提交**

  `git commit -m "refactor(config): 精简 worker yaml，删除 config.yaml"`

---

### 任务 9: master_ws_addr 推导

**文件：**
- 修改: `src/main.rs`（从 master_addr 推导 master_ws_addr）

- [ ] **步骤 1: 新增推导函数**

  在 `src/main.rs` 中新增：
  ```rust
  /// 从 master_addr 推导 master_ws_addr（同主机 + :50053）
  fn derive_master_ws_addr(master_addr: &str) -> String {
      // 解析 master_addr，提取 host 部分
      // "http://127.0.0.1:50051" -> "127.0.0.1:50053"
      let host = master_addr
          .trim_start_matches("http://")
          .trim_start_matches("https://")
          .split(':')
          .next()
          .unwrap_or("127.0.0.1");
      format!("{}:50053", host)
  }
  ```

- [ ] **步骤 2: run_worker 中使用推导**

  将 `&wc.master_ws_addr` 替换为 `&derive_master_ws_addr(&wc.master_addr)`。

- [ ] **步骤 3: 编写测试**

  ```rust
  #[test]
  fn test_derive_master_ws_addr() {
      assert_eq!(derive_master_ws_addr("http://127.0.0.1:50051"), "127.0.0.1:50053");
      assert_eq!(derive_master_ws_addr("http://192.168.1.10:50051"), "192.168.1.10:50053");
  }
  ```

- [ ] **步骤 4: 验证测试通过**

  `cargo test test_derive_master_ws_addr`
  预期: PASS

- [ ] **步骤 5: 提交**

  `git commit -m "refactor(worker): master_ws_addr 从 master_addr 推导"`

---

### 任务 10: 端到端验证

**文件：**
- 无（验证任务）

- [ ] **步骤 1: 启动 Master**

  `./target/release/store_system --config master.yaml`
  预期: Master 启动，监听 50051/50052/50053

- [ ] **步骤 2: 启动 Worker-0**

  `./target/release/store_system --config worker-0.yaml`
  预期: Worker 启动，日志中可见 `从 Master 获取配置: version=1, region=0, kv_ext=.g3db`

- [ ] **步骤 3: 验证 Worker 注册**

  通过 Admin API 查看 Worker 列表：
  `curl http://localhost:50052/api/workers`
  预期: worker-0 在线，region=0

- [ ] **步骤 4: 验证数据读写**

  通过客户端写入数据，验证路由到正确 Worker。

- [ ] **步骤 5: 验证热更新**

  修改 master.yaml 中 `cache_size`，重启 Master，观察 Worker 日志：
  预期: `热更新 cache_size: 10000 -> 20000`

- [ ] **步骤 6: 验证冷参数告警**

  修改 master.yaml 中 `kv_ext`，重启 Master，观察 Worker 日志：
  预期: `WARN 配置变更需重启生效: kv_ext .g3db -> .newdb`

- [ ] **步骤 7: 运行测试套件**

  `cargo test && make test-fault && make clean-data`
  预期: 全部通过

- [ ] **步骤 8: 提交**

  `git commit -m "test: 端到端验证 Master 统一配置管理"`

---

### 任务 11: README 更新

**文件：**
- 修改: `README.md`

- [ ] **步骤 1: 更新配置说明**

  在 README.md 中：
  1. 更新配置文件说明，移除 config.yaml 的描述
  2. 新增 master.yaml 的 `worker_defaults` / `worker_regions` 段说明
  3. 更新 worker-N.yaml 的说明（精简为 4 字段）
  4. 新增"配置管理架构"章节，说明 Master 统一管理的流程

- [ ] **步骤 2: 提交**

  `git commit -m "docs: 更新 README 说明 Master 统一配置管理"`

---

## 任务依赖关系

```
任务 1 (Proto) ─┬─> 任务 3 (MasterNode) ─┬─> 任务 4 (gRPC) ─> 任务 5 (Worker 注册)
                │                          │
任务 2 (Config) ┘                          ├─> 任务 6 (ConfigBroadcaster) ─> 任务 7 (Worker 接收)
                                           │
                                           └─> 任务 8 (配置文件) ─> 任务 9 (ws_addr 推导)
                                                                        │
                                                                        v
                                                                   任务 10 (E2E)
                                                                        │
                                                                        v
                                                                   任务 11 (README)
```

任务 1 和 2 可并行。任务 8 和 9 可在任务 5 完成后并行。
