# Store System

**版本**: 0.1.5

一个高性能的嵌入式键值存储系统，基于 bbolt (jammdb) + SQLite 双存储引擎，提供 gRPC 和 RESTful 双接口，支持写合并优化、WAL 原子写入、副本故障转移和大 Value（最大 100MB）读写。

## 特性

- **双存储引擎**：jammdb (B+树 KV) + SQLite (元数据，WAL 模式)
- **双接口**：gRPC (tonic) + RESTful (warp)
- **写合并优化**：put 先入内存缓冲，后台批量 fsync，写入延迟 < 1ms
- **WAL 原子写入**：Write-Ahead Log 三步协议 (WAL→KV→Meta)，崩溃恢复零数据丢失
- **副本故障转移**：Rendezvous Hashing 路由 + 同步写主副本 + 异步写备副本，Worker 宕机自动 fallback
- **大 Value 支持**：gRPC 消息上限 256MB，实测支持 100MB 单条读写
- **LRU 读缓存**：moka 自动淘汰，无需手动管理容量
- **批量事务**：元数据批量写入用真正的单事务提交 (put_batch_txn)
- **零拷贝优化**：gRPC 读取直接传递 protobuf bytes，避免字节拷贝
- **多节点集群**：Master-Worker 架构，支持动态注册/心跳/故障检测
- **QuadKey 区域路由**：按 quadkey[0] 将数据分流到 4 个区域 Worker，互不交叉
- **QuadKey 分片存储**：level≤8→base, 8<level<18→4位前缀, level≥18→8位前缀
- **动态数据类型路由**：RESTful `/{data_type}/{key}` 替代硬编码 `/objects/`
- **Master 统一配置**：kv_name/kv_ext/cache_size 等由 Master 统一下发 Worker
- **分片存储**：ShardManager 支持数据分片，独立 WAL 恢复
- **健康监控**：CPU/内存/磁盘使用率实时采集，心跳上报
- **日志采集**：Worker 日志通过 WebSocket 推送至 Master，SQLite 持久化

## 目录结构

```
StoreSystem/
├── Cargo.toml              # 服务器端依赖配置
├── build.rs                # protobuf 编译脚本
├── config.yaml             # 通用配置文件
├── master.yaml             # Master 节点配置
├── worker.yaml             # Worker-1 节点配置
├── worker2.yaml            # Worker-2 节点配置
├── proto/
│   └── store.proto         # gRPC 服务定义 (3 个 service)
├── src/
│   ├── main.rs             # 程序入口 (master/worker/standalone 模式)
│   ├── lib.rs              # 库入口，模块声明
│   ├── config.rs           # YAML 配置加载
│   ├── store.rs            # 核心存储层 (写合并 + LRU 缓存 + WAL 刷盘)
│   ├── kv.rs               # jammdb KV 存储封装
│   ├── meta.rs             # SQLite 元数据 (含 WAL 表 + 批量事务)
│   ├── shard.rs            # 分片管理器 (ShardManager + ShardStrategy)
│   ├── grpc.rs             # gRPC 服务实现 (StoreService)
│   ├── http.rs             # RESTful 服务实现
│   ├── master.rs           # Master 节点 (路由 + 副本 + 心跳)
│   ├── master_store.rs     # Master 集群元数据库
│   ├── master_admin_http.rs # Master 管理 API (集群概览/日志/Worker 列表)
│   ├── master_http.rs      # Master HTTP 客户端
│   ├── master_ws.rs        # Master WebSocket 客户端
│   ├── master_log_ws.rs    # Master 日志 WebSocket 服务端
│   ├── worker.rs           # Worker 节点 (存储 + WAL 恢复 + 心跳)
│   ├── worker_http.rs      # Worker RESTful API
│   ├── worker_ws.rs        # Worker WebSocket 服务端
│   ├── logger.rs           # 日志采集与持久化 (WorkerLogger + LogStore)
│   ├── health.rs           # 系统健康监控 (CPU/内存/磁盘)
│   └── error.rs            # 错误类型定义
├── client/                 # 性能测试客户端（独立项目）
│   ├── Cargo.toml
│   ├── build.rs
│   └── src/
│       ├── main.rs         # 测试主程序（多场景性能测试）
│       ├── bin/
│       │   └── fault_test.rs # 副本故障恢复测试
│       ├── grpc_client.rs  # gRPC 客户端
│       └── restful_client.rs # RESTful 客户端
└── admin-ui/               # Next.js 管理界面 (端口 3000)
```

## 快速开始

### 环境要求

- Rust 1.84+ (stable)
- macOS / Linux

### 编译

```bash
# 编译服务器
cargo build --release

# 编译客户端
cd client
cargo build --release
```

### 单机模式启动

```bash
./target/release/store_system --config config.yaml standalone
```

- gRPC 服务：`http://0.0.0.0:50051`
- RESTful 服务：`http://0.0.0.0:8080`
- 数据目录：`data/`（自动创建）

### 集群模式启动（QuadKey 4 区域）

```bash
# 终端 1: 启动 Master
./target/release/store_system --config master.yaml

# 终端 2-5: 启动 4 个区域 Worker
./target/release/store_system --config worker-0.yaml
./target/release/store_system --config worker-1.yaml
./target/release/store_system --config worker-2.yaml
./target/release/store_system --config worker-3.yaml
```

集群启动后端口分布：

| 节点 | region | gRPC | RESTful | 负责数据 |
|------|:------:|------|---------|---------|
| Master | — | 50051 | — | 路由分流 |
| Worker-0 | 0 | 50061 | 52061 | quadkey 0xxx |
| Worker-1 | 1 | 50062 | 52062 | quadkey 1xxx |
| Worker-2 | 2 | 50063 | 52063 | quadkey 2xxx |
| Worker-3 | 3 | 50064 | 52064 | quadkey 3xxx |

### 运行性能测试

```bash
cd client
./target/release/store_client
```

### 运行故障恢复测试

```bash
cd client
cargo build --release --bin fault_test
# 确保 Master + 2 Worker 已启动
../target/release/fault_test
```

## API 参考

### gRPC 接口

定义于 [proto/store.proto](proto/store.proto)：

| 方法 | 请求 | 响应 | 说明 |
|------|------|------|------|
| `Put` | `PutRequest{key, value, content_type, tags, quadkey?, level?}` | `PutResponse{meta}` | quadkey非空→区域路由，空→哈希路由 |
| `Get` | `GetRequest{key, quadkey?, level?}` | `GetResponse{value, meta}` | 同上 |
| `Delete` | `DeleteRequest{key, quadkey?, level?}` | `DeleteResponse{success}` | 同上 |
| `Exists` | `ExistsRequest{key, quadkey?, level?}` | `ExistsResponse{exists}` | 同上 |
| `List` | `ListRequest{prefix, limit, quadkey?, level?}` | `ListResponse{metas}` | 同上 |
| `PutBatch` | `PutBatchRequest{items[]}` | `PutBatchResponse{metas[]}` | items[] 每条含 quadkey, level |

### Master 管理接口 (gRPC)

| 方法 | 说明 |
|------|------|
| `RegisterWorker` | Worker 注册到集群 |
| `Heartbeat` | Worker 心跳上报 (含健康 + 写入统计) |
| `ListWorkers` | 获取所有 Worker 列表 |
| `GetRoute` | 查询 key 的路由目标 |

### RESTful 接口

路径模板：`/{data_type}/{key}?quadkey=&level=...`

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/{type}/{key}?quadkey=&level=&content_type=...` | 写入，quadkey可选 |
| `GET` | `/{type}/{key}?quadkey=&level=` | 读取 |
| `DELETE` | `/{type}/{key}?quadkey=&level=` | 删除 |
| `GET` | `/{type}?prefix=&limit=&quadkey=&level=` | 列表 |
| `POST` | `/{type}/batch` | 批量写入 |

### Master Admin API (RESTful)

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/overview` | 集群概览 |
| `GET` | `/api/v1/workers` | Worker 列表 |
| `GET` | `/api/v1/workers/:id` | Worker 详情 |
| `GET` | `/api/v1/logs?level=...&worker_id=...` | 日志查询 |
| `GET` | `/api/v1/logs/stats` | 日志统计 |
| `POST` | `/api/v1/logs/:id/ack` | 标记日志已读 |
| `GET` | `/api/v1/routes` | 路由规则 |
| `GET` | `/api/v1/health` | 健康检查 |
| `WS` | `/api/v1/ws/logs` | 实时日志流 |

## 架构设计

### 写合并（Write Coalescing）

核心优化，将同步 fsync 转为异步批量刷盘：

```
优化前（每次 put）:
  put → jammdb 开事务 → 写 → commit(fsync) → SQLite INSERT → 返回
  延迟：300ms+（两次 fsync）

优化后（每次 put）:
  put → 写内存 pending + 缓存 → 立即返回 (<1ms)

后台刷盘任务（每 5ms）+ WAL 原子保证:
  ① 写 WAL 意图记录 → ② jammdb 批量写(1次fsync) → ③ SQLite 批量INSERT + 原子清除WAL
```

**代价**：崩溃时 WAL 中的记录会在下次启动时自动恢复，数据零丢失。

### WAL 三步协议

```
客户端                    Worker
  │                          │
  │  put(key, value)         │
  │─────────────────────────→│  ① 写入 WriteBuffer(pending) + 返回
  │                          │
  │   后台 flusher 触发       │
  │                          │  ② 写 WAL (write_intent_wal 表)
  │                          │  ③ 写 KV (jammdb 单事务)
  │                          │  ④ 写 Meta + 原子清除 WAL
  │                          │
  │      崩溃恢复             │
  │                          │  扫描 write_intent_wal
  │                          │  ├─ KV 有数据 → 补写 Meta
  │                          │  └─ KV 无数据 → 丢弃 WAL 记录
```

### 副本写入 + 故障转移

```
                 Master
                   │
    Rendezvous Hashing 路由
    ┌──────────────────┴──────────────────┐
    │                                     │
    ▼                                     ▼
Worker-1 (主副本)                    Worker-2 (备副本)
    │ sync put                           async put
    │                                     │
    │←── 主副本成功 ──→ 返回 Client       │ (失败不影响主流程)
    │                                     │
    │   若主副本不可达:
    │       fallback → Worker-2 同步写入
    │       同时异步尝试 Worker-1
```

### 存储分层

```
┌─────────────────────────────────────────────────┐
│              gRPC (tonic) + RESTful (warp)       │
│                      ↓                          │
│              Master (quadkey[0]区域路由)         │
│    ┌──────┬──────┬──────┬──────┐                │
│    ▼      ▼      ▼      ▼                      │
│    W-0   W-1   W-2   W-3   (4 区域 Worker)     │
│  region0 region1 region2 region3                │
│    │      │      │      │                       │
│ 0xxx   1xxx   2xxx   3xxx  (quadkey分区)        │
│  ┌────────────────────────────┐                 │
│  │  LRU + WriteBuffer + WAL   │                 │
│  │  KvStore + MetaStore       │                 │
│  └────────────────────────────┘                 │
└─────────────────────────────────────────────────┘
```

### QuadKey 分片路径规则

| 层级 | DB 名 | 路径示例 |
|:----:|-------|---------|
| ≤ 8 | `base` | `{dir}/{type}/base.kv` |
| 9-17 | quadkey[..4] | `{dir}/{type}/12/3021.kv` |
| ≥ 18 | quadkey[..8] | `{dir}/{type}/20/30211234.kv` |

## v0.1.5 性能指标

### 测试环境

| 项目 | 配置 |
|------|------|
| **操作系统** | Linux |
| **Rust 版本** | 1.96+ |
| **CPU** | x86_64 |
| **测试日期** | 2026-06-21 |
| **集群** | Master + 4 Workers (QuadKey区域) |

### 完整性能测试报告

```
                              存储系统性能测试报告
测试项                                              次数     平均延迟(ms)      总耗时(ms)        ops/s         MB/s
----------------------------------------------------------------------------------------------------------
gRPC 单次写入 (1KB)                                  20        0.484        9.759       2049.4          2.0
gRPC 单次写入 (1MB)                                  10        6.442       72.177        138.5        138.5
gRPC 单次写入 (10MB)                                  5      120.803      642.216          7.8         77.9
gRPC 单次写入 (50MB)                                  3      469.939     1551.186          1.9         96.7
gRPC 单次写入 (100MB)                                 2     1024.315     2258.421          0.9         88.6
gRPC 单次读取 (1KB)                                  20        2.564       51.287        390.0          0.4
gRPC 单次读取 (1MB)                                  10        6.576       65.755        152.1        152.1
gRPC 单次读取 (10MB)                                  5       62.251      311.256         16.1        160.6
gRPC 单次读取 (50MB)                                  3      737.287     2211.860          1.4         67.8
gRPC 单次读取 (100MB)                                 2     2326.428     4652.855          0.4         43.0
gRPC 批量写入 (1MB x 10条/批)                           5       55.363      285.838         17.5        174.9
gRPC 批量写入 (10MB x 5条/批)                           3      431.536     1386.104          2.2        108.2
gRPC 并发写入 (1MB x 10并发)                           20       52.265      228.563         87.5         87.5
gRPC 并发写入 (10MB x 5并发)                           10      218.866      549.959         18.2        181.8
RESTful 单次写入 (1KB)                               20        0.222        4.480       4464.3          4.4
RESTful 单次写入 (1MB)                               10        1.696       17.037        586.9        586.9
RESTful 单次写入 (10MB)                               5       14.734       75.147         66.5        665.4
RESTful 单次写入 (50MB)                               3      120.959      394.350          7.6        380.4
RESTful 单次写入 (100MB)                              2      220.817      508.098          3.9        393.6
RESTful 单次读取 (1KB)                               20        0.140        2.801       7141.2          7.0
RESTful 单次读取 (1MB)                               10        6.175       61.750        161.9        161.9
RESTful 单次读取 (10MB)                               5       71.143      355.716         14.1        140.6
RESTful 单次读取 (50MB)                               3      469.231     1407.694          2.1        106.6
RESTful 单次读取 (100MB)                              2     1709.396     3418.791          0.6         58.5
RESTful 批量写入 (1MB x 10条/批)                        5       79.520      410.060         12.2        121.9
RESTful 批量写入 (10MB x 5条/批)                        3      736.548     2413.489          1.2         62.2
RESTful 并发写入 (1MB x 10并发)                        20       17.778      781.843         25.6         25.6
RESTful 并发写入 (10MB x 5并发)                        10      309.105     2378.779          4.2         42.0
```

## 配置说明

所有配置项通过 YAML 文件管理，无需修改程序代码。

### 运行模式

```yaml
# config.yaml
mode: master          # master | worker | standalone
```

### Master 配置

```yaml
master:
  listen_addr: "0.0.0.0:50051"
  meta_path: "master_data/master.db"
  heartbeat_timeout_secs: 30       # Worker 心跳超时
  cleanup_interval_secs: 60        # 宕机 Worker 清理间隔
```

### Worker 配置

```yaml
worker:
  worker_id: "worker-1"
  listen_addr: "0.0.0.0:50061"
  master_addr: "http://127.0.0.1:50051"
  data_dir: "worker_data/worker-1"
  cache_size: 10000                # LRU 缓存容量
  flush_interval_ms: 5             # 刷盘间隔
  heartbeat_interval_secs: 10      # 心跳间隔
  weight: 1                        # 负载均衡权重
```

### gRPC 消息大小

```yaml
global:
  max_message_size: 268435456      # 256MB
  protocol: "both"                 # grpc | restful | ws | both
```

## 数据持久化

- `data/kv.db` / `worker_data/{id}/kv.db`：jammdb KV 数据（B+树，mmap）
- `data/meta.db` / `worker_data/{id}/meta.db`：SQLite 元数据（WAL 模式）
- `write_intent_wal` 表：WAL 意图日志（崩溃恢复用）

**崩溃恢复**：
- jammdb 自身保证 ACID
- WAL 三步协议保证 KV 和 Meta 的一致性
- 启动时自动扫描 `write_intent_wal` 表，补写未完成的 Meta 记录
- v0.1.1 起：崩溃后数据**零丢失**（不再丢失 5ms 未刷盘数据）

## 开发

### 运行测试

```bash
# 性能测试
cargo run --release  # 启动服务
cd client && cargo run --release  # 运行客户端

# 故障恢复测试
cd client
cargo build --release --bin fault_test
../target/release/fault_test
```

### 清理数据

```bash
rm -rf data/ master_data/ worker_data/
```

## 依赖说明

| 依赖 | 版本 | 用途 |
|------|------|------|
| jammdb | 0.11 | bbolt 的 Rust 移植，B+树 KV |
| rusqlite | 0.29 | SQLite 绑定（bundled） |
| tonic | 0.9 | gRPC 框架 |
| warp | 0.3 | RESTful 框架 |
| tokio | 1.36 | 异步运行时 |
| moka | 0.12 | LRU 线程安全缓存（v0.1.1 新增） |
| dashmap | 5.4 | 线程安全哈希表 |
| bytes | 1.0 | 零拷贝字节处理 |
| chrono | 0.4 | 时间处理 |
| seahash | 1.0 | Rendezvous Hashing 路由 |
| serde_yaml | 0.9 | YAML 配置解析 |
| reqwest | 0.11 | HTTP 客户端（Master→Worker） |
| tokio-tungstenite | 0.21 | WebSocket（Worker 日志推送） |
| num_cpus | 1.0 | CPU 核心数检测 |
| libc | 0.2 | 系统调用（statvfs） |

## 许可证

私有项目，未开源。
