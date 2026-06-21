# QuadKey 区域路由 — 实现计划

> **目标：** 集群按 quadkey[0] 分成 4 区域，每区域一个 Worker

## 全局约束

- Master 路由算法改为 `quadkey[0] → region → Worker`
- Worker 数固定 4（0/1/2/3），每 Worker 声明自己的 `region`
- 本级现有 `Rendezvous Hashing` 保留用于非 quadkey 请求
- 现有副本机制不变

---

### 任务 1: Worker 配置 + 身份

**文件：** `worker.yaml` → `worker-0.yaml`、`worker-1.yaml`、`worker-2.yaml`、`worker-3.yaml`

- [ ] **Step 1: 创建 4 个 Worker 配置文件**
  ```yaml
  worker:
    worker_id: "worker-0"
    region: "0"              # 新增
    listen_addr: "0.0.0.0:50061"
    data_dir: "worker_data/worker-0"
    ...
  ```
  端口分配: 50061, 50062, 50063, 50064
  RESTful:  52061, 52062, 52063, 52064

- [ ] **Step 2: WorkerConfig 添加 region 字段**
  `src/config.rs` WorkerConfig: `pub region: String`

---

### 任务 2: Master 按 quadkey[0] 路由

**文件：** `src/master.rs`

- [ ] **Step 1: 新增 `route_by_quadkey()` 方法**
  ```rust
  pub async fn route_by_quadkey(&self, quadkey: &str) -> Result<WorkerInfo> {
      let region = &quadkey[0..1];
      let workers = self.workers.read().await;
      workers.values()
          .find(|w| w.region == region && w.alive)
          .cloned()
          .ok_or_else(|| StoreError::InvalidArgument(
              format!("区域 {} 无可用 Worker", region)))
  }
  ```

- [ ] **Step 2: MasterStore 添加 region 字段**
  `WorkerInfo` 加 `pub region: String`
  SQLite `worker_registry` 表加 `region TEXT`

- [ ] **Step 3: put/get/delete handler 中优先 quadkey 路由**
  在 `MasterStoreService` handler 中：
  ```
  if req.quadkey 非空:
      worker = master.route_by_quadkey(&req.quadkey)
  else:
      worker = master.route(&req.key)  // 现有 Rendezvous
  ```

---

### 任务 3: 启动 + 测试

**文件：** `src/main.rs`、Makefile、集成测试

- [ ] **Step 1: main.rs 打印 Worker 区域**
- [ ] **Step 2: Makefile 更新 worker 端口**
- [ ] **Step 3: 集成测试 — 4 Worker 集群 + quadkey 路由**

---

### 任务 4: 验证 + 文档

- [ ] `cargo build --release`
- [ ] `cargo test`
- [ ] 4 Worker 集群启动 + quadkey 路由测试
- [ ] 一个 Worker 宕机后对应区域不可用（预期行为）
