# 存储系统性能测试报告

**测试日期**: 2026-06-21
**存储引擎**: jammdb (bbolt) + SQLite (WAL 模式) + QuadShard 分片
**测试客户端**: client/ 目录独立实现，不依赖服务端代码

## 1. 测试环境

| 项目 | 配置 |
|------|------|
| 操作系统 | Linux |
| KV 数据库 | jammdb (bbolt 的 Rust 移植) |
| 元数据存储 | rusqlite (WAL 模式) |
| gRPC 框架 | tonic 0.12 |
| RESTful 框架 | warp 0.3 |
| 分片引擎 | QuadShard (base_level=8, split_level=18) |

## 2. 架构说明

```
Client (测试工具)
  ├── gRPC → Master(:50051) → Worker(quadkey路由) → QuadShard 分片存储
  └── RESTful → Worker(:8080) 直连 → QuadShard 分片存储
```

- **gRPC 路径**: 通过 Master 的 gRPC 接口，Master 根据 quadkey 首字符路由到对应区域的 Worker
- **RESTful 路径**: 直连 Worker 的 HTTP 接口，Worker 内部根据 quadkey + level 路由到具体 DB 文件
- **所有请求均携带 quadkey + level 参数**，符合新的 quadkey 区域路由规则

## 3. 测试场景

### 3.1 基础性能测试

| 测试项 | 说明 |
|--------|------|
| 单次写入延迟 | 不同数据大小 (1KB, 1MB, 10MB) 的单次写入延迟 |
| 批量写入延迟 | 不同批量大小 (10, 50, 100 条/批) 的批量写入延迟 |
| 并发写入延迟 | 不同并发数 (10, 50) 的并发写入延迟 |

### 3.2 高压测试

| 测试项 | 说明 |
|--------|------|
| 高压写入 (1KB) | 50并发 × 1000条 × 1KB |
| 高压写入 (1MB) | 50并发 × 500条 × 1MB |
| 混合读写 | 20读并发 + 20写并发 × 50次 |
| 长时间稳定性 | 10并发 × 30秒 × 1KB，每秒报告吞吐量 |

### 3.3 路由验证

| 测试项 | 说明 |
|--------|------|
| QuadKey 路由验证 | 向 4 个区域 (0/1/2/3) 写入并读取，验证路由正确性 |

## 4. 测试代码位置

- **客户端代码**: [client/](client/)
- **gRPC 客户端**: [client/src/grpc_client.rs](client/src/grpc_client.rs)
- **RESTful 客户端**: [client/src/restful_client.rs](client/src/restful_client.rs)
- **测试主程序**: [client/src/main.rs](client/src/main.rs)

## 5. 复现方法

```bash
# 1. 启动服务端（Master + 4 个 Worker）
cd /home/stone/Downloads/store-0.1.0
# 启动 Master
cargo run -- --config master.yaml
# 启动 Worker（4 个终端）
cargo run -- --config worker-0.yaml
cargo run -- --config worker-1.yaml
cargo run -- --config worker-2.yaml
cargo run -- --config worker-3.yaml

# 2. 运行客户端测试
cd client
cargo run --release
```

## 6. 关键特性

### 6.1 QuadKey 区域路由

- 每个 key 通过 hash 生成 quadkey（`h % 4` → `"0"/"1"/"2"/"3"`）
- Master 根据 quadkey 首字符路由到对应区域的 Worker
- Worker 内部使用 `QuadShardManager` 根据 quadkey + level 路由到具体 DB 文件
- 数据库扩展名：KV 为 `.g3db`，Meta 为 `.bulk`

### 6.2 高压测试能力

- **并发控制**: 支持指定并发数，使用 tokio::spawn 实现
- **延迟统计**: 记录每次请求的延迟，计算平均/最小/最大延迟
- **吞吐报告**: 每秒报告实时吞吐量 (ops/s, MB/s)
- **错误统计**: 记录成功/失败次数
- **混合负载**: 同时进行读写操作，模拟真实场景

### 6.3 稳定性测试

- 持续写入指定时长（默认 30 秒）
- 每秒报告实时吞吐量
- 使用 AtomicU64 进行无锁计数
- 支持指定并发数和数据大小
