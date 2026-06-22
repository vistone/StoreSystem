# Epoch 桶实现计划

**目标：** 将 `QuadShardConfig.data_type`（固定值 "objects"）替换为客户端传入的 `epoch`（版本号）作为桶名

**架构：** 数据提交时客户端携带 epoch 字段，系统将其作为存储路径中的桶名，路径从 `quad_data/objects/{level}/{prefix}.kv` 变为 `quad_data/{epoch}/{level}/{prefix}.kv`

**技术栈：** Rust + protobuf + gRPC

## 全局约束

- 每次只改一个模块 → 编译 → 测试 → 通过才继续
- 禁止重写现有代码，使用最小 diff
- 使用 `anyhow`/`thiserror` 进行错误处理
- 公共 API 必须有文档注释

---

### 任务 1: proto 增加 epoch 字段

**文件：**
- 修改: `proto/store.proto`

- [ ] **步骤 1: PutRequest 增加 epoch 字段**
- [ ] **步骤 2: GetRequest 增加 epoch 字段**
- [ ] **步骤 3: DeleteRequest 增加 epoch 字段**
- [ ] **步骤 4: ExistsRequest 增加 epoch 字段**
- [ ] **步骤 5: ListRequest 增加 epoch 字段**
- [ ] **步骤 6: BatchItem 增加 epoch 字段**

### 任务 2: config.rs 移除 data_type

**文件：**
- 修改: `src/config.rs`

- [ ] **步骤 1: QuadShardConfig 移除 data_type 字段**
- [ ] **步骤 2: 更新默认值函数**

### 任务 3: quad_shard.rs 增加 epoch 参数

**文件：**
- 修改: `src/quad_shard.rs`

- [ ] **步骤 1: `new()` 移除 data_type 路径创建**
- [ ] **步骤 2: `route_paths()` 增加 epoch 参数，路径使用 epoch**
- [ ] **步骤 3: `route()` 增加 epoch 参数**
- [ ] **步骤 4: `put()` 增加 epoch 参数**
- [ ] **步骤 5: `get()` 增加 epoch 参数**
- [ ] **步骤 6: `delete()` 增加 epoch 参数**
- [ ] **步骤 7: `exists()` 增加 epoch 参数**
- [ ] **步骤 8: `list()` 增加 epoch 参数**
- [ ] **步骤 9: 更新测试用例**

### 任务 4: worker.rs 传递 epoch

**文件：**
- 修改: `src/worker.rs`

- [ ] **步骤 1: WorkerNode 的 quad_shard 相关方法增加 epoch 参数**
- [ ] **步骤 2: 更新 put_object 等方法的调用链**

### 任务 5: grpc.rs 传递 epoch

**文件：**
- 修改: `src/grpc.rs`

- [ ] **步骤 1: GrpcStoreService 的 put/get 等方法传递 epoch**

### 任务 6: master.rs 传递 epoch

**文件：**
- 修改: `src/master.rs`

- [ ] **步骤 1: MasterStoreService 的 put_via_quadkey 等方法传递 epoch**

### 任务 7: worker_http.rs 解析 epoch

**文件：**
- 修改: `src/worker_http.rs`

- [ ] **步骤 1: PutQuery 增加 epoch 字段**
- [ ] **步骤 2: put_handler 传递 epoch 到 quad_shard**

### 任务 8: worker_ws.rs 解析 epoch

**文件：**
- 修改: `src/worker_ws.rs`

- [ ] **步骤 1: PutPayload 增加 epoch 字段**
- [ ] **步骤 2: process_message 传递 epoch**

### 任务 9: client 适配 epoch

**文件：**
- 修改: `client/src/grpc_client.rs`
- 修改: `client/src/main.rs`

- [ ] **步骤 1: grpc_client.rs 的 put/get 等方法增加 epoch 参数**
- [ ] **步骤 2: main.rs 更新测试流程**

### 任务 10: config.yaml 更新

**文件：**
- 修改: `config.yaml`

- [ ] **步骤 1: 移除 data_type 配置项**

### 任务 11: 编译验证

- [ ] **步骤 1: `cargo build --release` 编译通过**
- [ ] **步骤 2: `cargo test` 全部通过**
