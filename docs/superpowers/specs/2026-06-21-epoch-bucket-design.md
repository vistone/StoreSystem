# Epoch 桶设计 — 用版本号替代 objects 数据类型

## 问题

当前 `QuadShardConfig` 中 `data_type` 默认值为 `"objects"`，导致存储路径为：

```
quad_data/objects/{level}/{prefix}.kv
```

用户指出"没有什么 objects 这样的数据类型和桶名"。数据应该按 **epoch（版本号）** 组织，每个 epoch 就是一个版本桶。

## 设计

### 核心变更

1. **移除 `data_type` 概念** — 不再有 `"objects"` 这样的固定数据类型
2. **epoch 由客户端传入** — 每次数据提交时，请求中携带 `epoch` 字段
3. **epoch 作为桶名** — 存储路径改为 `quad_data/{epoch}/{level}/{prefix}.kv`

### 存储路径变化

```
之前: quad_data/objects/{level}/{prefix}.kv
之后: quad_data/{epoch}/{level}/{prefix}.kv
```

例如 epoch="v1", level=12, quadkey="30211234"：
```
之前: quad_data/objects/12/3021.kv
之后: quad_data/v1/12/3021.kv
```

### 接口变更

#### proto 变更

`PutRequest` 增加 `epoch` 字段：
```protobuf
message PutRequest {
  string key = 1;
  bytes value = 2;
  string content_type = 3;
  string tags = 4;
  string quadkey = 5;
  uint32 level = 6;
  string epoch = 7;  // 新增：版本号/桶名
}
```

`GetRequest`、`DeleteRequest`、`ExistsRequest`、`ListRequest`、`BatchItem` 同样增加 `epoch` 字段。

#### QuadShardManager 变更

所有方法增加 `epoch` 参数：
- `put(epoch, quadkey, level, key, value, ...)`
- `get(epoch, quadkey, level, key)`
- `delete(epoch, quadkey, level, key)`
- `exists(epoch, quadkey, level, key)`
- `list(epoch, quadkey, level, prefix, limit)`

路径生成逻辑：
```rust
// 之前
PathBuf::from(&config.data_dir).join(&config.data_type).join(level.to_string())

// 之后
PathBuf::from(&config.data_dir).join(epoch).join(level.to_string())
```

#### Config 变更

`QuadShardConfig` 移除 `data_type` 字段，新增 `epoch` 默认值（向后兼容）：
```rust
pub struct QuadShardConfig {
    pub base_level: u32,
    pub split_level: u32,
    pub data_dir: String,     // 数据根目录，如 "quad_data"
    // data_type 已移除，由 epoch 替代
    pub kv_ext: String,
    pub meta_ext: String,
    pub cache_size: usize,
    pub flush_interval_ms: u64,
}
```

### 数据流

```
客户端 (epoch="v1") 
  → gRPC PutRequest { key, value, quadkey, level, epoch="v1" }
  → Master (路由到区域 Worker)
  → Worker (调用 QuadShardManager::put("v1", quadkey, level, key, value))
  → 存储到 quad_data/v1/{level}/{prefix}.kv
```

### 向后兼容

- 如果客户端不传 `epoch`，默认使用 `"default"` 作为 epoch
- 配置文件中 `data_type` 字段仍然可以存在但会被忽略（或作为警告）
- 旧的 `quad_data/objects/` 目录数据不受影响

### 影响范围

| 文件 | 改动 |
|------|------|
| `proto/store.proto` | 所有请求消息增加 `epoch` 字段 |
| `src/config.rs` | 移除 `data_type`，调整默认值 |
| `src/quad_shard.rs` | 所有方法增加 `epoch` 参数，路径使用 epoch |
| `src/worker.rs` | `put_object` 等方法传递 epoch |
| `src/worker_http.rs` | HTTP handler 解析 epoch 参数 |
| `src/worker_ws.rs` | WebSocket handler 解析 epoch 参数 |
| `src/grpc.rs` | gRPC handler 传递 epoch |
| `src/master.rs` | Master 路由传递 epoch |
| `client/src/grpc_client.rs` | 测试客户端增加 epoch 参数 |
| `client/src/main.rs` | 测试主流程适配 |
| `config.yaml` | 移除 `data_type` 配置项 |
