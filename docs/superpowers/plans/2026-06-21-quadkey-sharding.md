# QuadKey 分片 — 实现计划

> **目标：** 将数据按 quadkey 路由到 `{data_dir}/{data_type}/{level}/{prefix}.{ext}` 路径

## 全局约束

- `data_type` 可配置，所有路径中不再硬编码 `objects`
- quadkey/level 为 optional 字段，不传走现有 Rendezvous Hashing（完全向后兼容）
- 层级 ≤ 8 → `base`，8 < 层级 < 18 → quadkey 前 4 位，层级 ≥ 18 → quadkey 前 8 位
- 现有 `worker_data/` 目录结构和 Master-Worker 集群不受影响

---

### 任务 1: proto + config — 新增 quadkey 字段

**文件：**
- 修改: `proto/store.proto`
- 修改: `src/config.rs`

**接口：**
- 产出: `PutRequest { quadkey, level }`, `GetRequest { quadkey, level }` 等
- 产出: `QuadShardConfig { base_level, split_level, data_dir, data_type, kv_ext, meta_ext, ... }`

- [ ] **Step 1: proto 新增字段**
  给 `PutRequest`, `GetRequest`, `DeleteRequest`, `ExistsRequest`, `ListRequest`, `BatchItem` 加 `quadkey` + `level`

- [ ] **Step 2: config.rs 新增 QuadShardConfig**
  ```rust
  pub struct QuadShardConfig {
      pub base_level: u32,    // 默认 8
      pub split_level: u32,   // 默认 18
      pub data_dir: String,
      pub data_type: String,
      pub kv_ext: String,
      pub meta_ext: String,
      pub cache_size: usize,
      pub flush_interval_ms: u64,
  }
  ```

- [ ] **Step 3: cargo build 验证 proto 生成正确**

---

### 任务 2: QuadShardManager 核心

**文件：**
- 创建: `src/quad_shard.rs`

**接口：**
- 产出: `QuadShardManager::route(quadkey, level) → (db_name, kv_path, meta_path)`
- 产出: `QuadShardManager::put/get/delete/exists/list`

- [ ] **Step 1: route 算法**
  ```rust
  fn route(&self, quadkey: &str, level: u32) -> (String, PathBuf, PathBuf) {
      let db_name = if level <= self.config.base_level { "base".to_string() }
                    else if level < self.config.split_level { quadkey[..4].to_string() }
                    else { quadkey[..8].to_string() };
      let dir = if level <= self.config.base_level {
          PathBuf::from(&self.config.data_dir).join(&self.config.data_type)
      } else {
          PathBuf::from(&self.config.data_dir)
              .join(&self.config.data_type)
              .join(level.to_string())
      };
      let kv_path = dir.join(format!("{}.{}", db_name, self.config.kv_ext.trim_start_matches('.')));
      let meta_path = dir.join(format!("{}.{}", db_name, self.config.meta_ext.trim_start_matches('.')));
      (db_name, kv_path, meta_path)
  }
  ```

- [ ] **Step 2: lazy open — 首次写入时打开 DB**
  `shard_cache: DashMap<(String, u32), Arc<Shard>>` — key = (db_name, level)

- [ ] **Step 3: put/get/delete/exists/list + 后台刷盘 + WAL**

- [ ] **Step 4: cargo test 验证**

---

### 任务 3: gRPC handler — quadkey 路由分支

**文件：**
- 修改: `src/grpc.rs`
- 修改: `src/worker.rs`

**接口：**
- 消费: proto 的 quadkey/level 字段

- [ ] **Step 1: GrpcStoreService handler 适配**
  在 put/get/delete/exists/list/put_batch 中，检查 `req.quadkey`：
  - 非空 → 调用 `QuadShardManager` 路由
  - 空 → 走现有 `WorkerNode` 路径

- [ ] **Step 2: WorkerService handler 适配**
  同上

- [ ] **Step 3: cargo build + cargo test 全量验证**

---

### 任务 4: RESTful handler — 动态 data_type 路由

**文件：**
- 修改: `src/http.rs`
- 修改: `src/worker_http.rs`

- [ ] **Step 1: warp 路由改为 `/{data_type}/{key}`**
  当前写死 `/objects/`，改为动态段 `/:data_type/:key`

- [ ] **Step 2: handler 中读取 quadkey/level query 参数**
  ```rust
  struct PutQuery {
      content_type: Option<String>,
      tags: Option<String>,
      quadkey: Option<String>,
      level: Option<u32>,
  }
  ```

- [ ] **Step 3: cargo test 验证**

---

### 任务 5: 集成 + 测试 + 文档

**文件：**
- 修改: `config.yaml`
- 创建: `src/quad_shard.rs` 的测试

- [ ] **Step 1: config.yaml 加 quad_shard 配置段**

- [ ] **Step 2: 单元测试 — 各种 quadkey/level 组合的路径生成**

- [ ] **Step 3: 集成测试 — gRPC 走 quadkey 写入+读出**

- [ ] **Step 4: README 更新**
