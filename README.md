# Store System

**版本**: 0.1.0

一个高性能的嵌入式键值存储系统，基于 bbolt (jammdb) + SQLite 双存储引擎，提供 gRPC 和 RESTful 双接口，支持写合并优化和大 Value（最大 100MB）读写。

## 特性

- **双存储引擎**：jammdb (B+树 KV) + SQLite (元数据，WAL 模式)
- **双接口**：gRPC (tonic) + RESTful (warp)
- **写合并优化**：put 先入内存缓冲，后台批量 fsync，写入延迟 < 1ms
- **大 Value 支持**：gRPC 消息上限 256MB，实测支持 100MB 单条读写
- **读缓存**：DashMap 热点缓存，读取命中缓存 < 1ms
- **批量事务**：元数据批量写入用单事务提交，避免逐条 commit
- **零拷贝优化**：gRPC 读取直接传递 protobuf bytes，避免字节拷贝

## 目录结构

```
Store/
├── Cargo.toml              # 服务器端依赖配置
├── build.rs                # protobuf 编译脚本
├── proto/
│   └── store.proto         # gRPC 服务定义
├── src/
│   ├── main.rs             # 程序入口，启动 gRPC + RESTful 服务
│   ├── lib.rs              # 库入口，模块声明
│   ├── store.rs            # 核心存储层（写合并 + 缓存 + 刷盘）
│   ├── kv.rs               # jammdb KV 存储封装
│   ├── meta.rs             # SQLite 元数据存储（含批量事务）
│   ├── grpc.rs             # gRPC 服务实现
│   ├── http.rs             # RESTful 服务实现
│   └── error.rs            # 错误类型定义
├── client/                 # 性能测试客户端（独立项目）
│   ├── Cargo.toml
│   ├── build.rs
│   └── src/
│       ├── main.rs         # 测试主程序（多场景性能测试）
│       ├── grpc_client.rs  # gRPC 客户端
│       └── restful_client.rs # RESTful 客户端
└── .gitignore
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

### 启动服务

```bash
./target/release/store_system
```

服务启动后：
- gRPC 服务：`http://0.0.0.0:50051`
- RESTful 服务：`http://0.0.0.0:8080`
- 数据目录：`data/`（自动创建）

### 运行性能测试

```bash
cd client
./target/release/store_client
```

## API 参考

### gRPC 接口

定义于 [proto/store.proto](proto/store.proto)：

| 方法 | 请求 | 响应 | 说明 |
|------|------|------|------|
| `Put` | `PutRequest{key, value, content_type, tags}` | `PutResponse{meta}` | 写入单条 |
| `Get` | `GetRequest{key}` | `GetResponse{value, meta}` | 读取单条 |
| `Delete` | `DeleteRequest{key}` | `DeleteResponse{success}` | 删除单条 |
| `Exists` | `ExistsRequest{key}` | `ExistsResponse{exists}` | 检查存在 |
| `List` | `ListRequest{prefix, limit}` | `ListResponse{metas}` | 按前缀列表 |
| `PutBatch` | `PutBatchRequest{items[]}` | `PutBatchResponse{metas[]}` | 批量写入 |

### RESTful 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/objects/{key}?content_type=...` | 写入（body 为 raw bytes） |
| `GET` | `/objects/{key}` | 读取（返回 JSON，value 为 base64） |
| `DELETE` | `/objects/{key}` | 删除 |
| `GET` | `/objects?prefix=...&limit=...` | 列表 |
| `POST` | `/objects/batch` | 批量写入（JSON body） |

## 架构设计

### 写合并（Write Coalescing）

核心优化，将同步 fsync 转为异步批量刷盘：

```
优化前（每次 put）:
  put → jammdb 开事务 → 写 → commit(fsync) → SQLite INSERT → 返回
  延迟：300ms+（两次 fsync）

优化后（每次 put）:
  put → 写内存 pending + 缓存 → 立即返回 (<1ms)

后台刷盘任务（每 5ms）:
  drain pending → jammdb 单事务批量写(1次fsync) → SQLite 单事务批量INSERT
```

**代价**：崩溃时最多丢失最近 5ms 未刷盘数据。需要强持久性时调用 `store.flush()` 同步等待。

### 存储分层

```
┌─────────────────────────────────────────┐
│           gRPC (tonic)                  │
│              ↓                          │
│         RESTful (warp)                  │
├─────────────────────────────────────────┤
│              Store 层                    │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │  读缓存     │  │  写合并缓冲区     │  │
│  │ (DashMap)   │  │ (WriteBuffer)    │  │
│  └─────────────┘  └────────┬─────────┘  │
│                            │ 后台刷盘    │
│  ┌─────────────────────────┴──────────┐ │
│  │  KvStore (jammdb)  +  MetaStore    │ │
│  │    B+树 KV 数据      SQLite 元数据  │ │
│  └────────────────────────────────────┘ │
└─────────────────────────────────────────┘
```

## 性能指标

### 测试环境

| 项目 | 配置 |
|------|------|
| **操作系统** | Linux 7.0 |
| **Rust 版本** | 1.84+ |
| **CPU** | x86_64 |
| **测试日期** | 2026-06-16 |

### 完整性能测试报告

以下为 Linux 环境下 28 项性能测试的完整结果，覆盖 gRPC 和 RESTful 两种协议，value 大小从 1KB 到 100MB。

```
                              存储系统性能测试报告
测试项                                              次数     平均延迟(ms)      总耗时(ms)        ops/s         MB/s
----------------------------------------------------------------------------------------------------------
gRPC 单次写入 (1KB)                                  20        0.352        7.080       2824.8          2.8
gRPC 单次写入 (1MB)                                  10        9.342      107.421         93.1         93.1
gRPC 单次写入 (10MB)                                  5       88.489      550.531          9.1         90.8
gRPC 单次写入 (50MB)                                  3      394.703     1428.393          2.1        105.0
gRPC 单次写入 (100MB)                                 2      631.073     1585.418          1.3        126.1
gRPC 单次读取 (1KB)                                  20        0.373        7.458       2681.5          2.6
gRPC 单次读取 (1MB)                                  10        4.666       46.656        214.3        214.3
gRPC 单次读取 (10MB)                                  5       62.031      310.157         16.1        161.2
gRPC 单次读取 (50MB)                                  3      513.476     1540.428          1.9         97.4
gRPC 单次读取 (100MB)                                 2      754.910     1509.819          1.3        132.5
gRPC 批量写入 (1MB x 10条/批)                           5      132.505      674.001          7.4         74.2
gRPC 批量写入 (10MB x 5条/批)                           3      289.823     1045.661          2.9        143.4
gRPC 并发写入 (1MB x 10并发)                           20       91.482      194.965        102.6        102.6
gRPC 并发写入 (10MB x 5并发)                           10      231.552      574.344         17.4        174.1
RESTful 单次写入 (1KB)                               20        9.253      185.096        108.1          0.1
RESTful 单次写入 (1MB)                               10        2.993       30.148        331.7        331.7
RESTful 单次写入 (10MB)                               5       20.543      104.545         47.8        478.3
RESTful 单次写入 (50MB)                               3      176.805      602.048          5.0        249.1
RESTful 单次写入 (100MB)                              2      390.715      881.364          2.3        226.9
RESTful 单次读取 (1KB)                               20        0.177        3.531       5664.7          5.5
RESTful 单次读取 (1MB)                               10       11.573      115.727         86.4         86.4
RESTful 单次读取 (10MB)                               5      113.230      566.149          8.8         88.3
RESTful 单次读取 (50MB)                               3      681.742     2045.226          1.5         73.3
RESTful 单次读取 (100MB)                              2     1234.743     2469.487          0.8         81.0
RESTful 批量写入 (1MB x 10条/批)                        5      177.558      901.666          5.5         55.5
RESTful 批量写入 (10MB x 5条/批)                        3      460.712     1640.478          1.8         91.4
RESTful 并发写入 (1MB x 10并发)                        20       15.825      314.056         63.7         63.7
RESTful 并发写入 (10MB x 5并发)                        10       36.067      228.770         43.7        437.1
```

### 大 Value 写入吞吐（MB/s）

| Value 大小 | gRPC 写入 | RESTful 写入 |
|-----------|:---------:|:------------:|
| 1KB | 2.8 MB/s | 0.1 MB/s |
| 1MB | 93.1 MB/s | **331.7 MB/s** |
| 10MB | 90.8 MB/s | **478.3 MB/s** |
| 50MB | 105.0 MB/s | 249.1 MB/s |
| 100MB | 126.1 MB/s | 226.9 MB/s |

### 大 Value 读取吞吐（MB/s）

| Value 大小 | gRPC 读取 | RESTful 读取 |
|-----------|:---------:|:------------:|
| 1KB | 2.6 MB/s | **5.5 MB/s** |
| 1MB | **214.3 MB/s** | 86.4 MB/s |
| 10MB | **161.2 MB/s** | 88.3 MB/s |
| 50MB | **97.4 MB/s** | 73.3 MB/s |
| 100MB | **132.5 MB/s** | 81.0 MB/s |

### 批量写入吞吐（MB/s）

| 测试项 | 吞吐量 |
|--------|:------:|
| gRPC 批量写入 (1MB x 10条/批) | 74.2 MB/s |
| gRPC 批量写入 (10MB x 5条/批) | **143.4 MB/s** |
| RESTful 批量写入 (1MB x 10条/批) | 55.5 MB/s |
| RESTful 批量写入 (10MB x 5条/批) | 91.4 MB/s |

### 并发写入吞吐（MB/s）

| 测试项 | 吞吐量 |
|--------|:------:|
| gRPC 并发写入 (1MB x 10并发) | 102.6 MB/s |
| gRPC 并发写入 (10MB x 5并发) | 174.1 MB/s |
| RESTful 并发写入 (1MB x 10并发) | 63.7 MB/s |
| RESTful 并发写入 (10MB x 5并发) | **437.1 MB/s** |

### 协议选择建议

| 场景 | 推荐协议 | 原因 |
|------|---------|------|
| 大 Value 写入 | RESTful | body 透传，无 protobuf 开销，10MB 写入达 478.3 MB/s |
| 大 Value 读取 | gRPC | protobuf bytes 零拷贝，避免 base64，1MB 读取达 214.3 MB/s |
| 小 Value 高频读 | RESTful | 延迟更低，1KB 读取仅 0.177ms |
| 批量写入 | gRPC | protobuf 二进制编码高效，10MB 批量达 143.4 MB/s |
| 并发写入 | RESTful | 10MB 并发写入达 437.1 MB/s |
| 跨语言调用 | gRPC | 强类型 proto 契约 |
| Web 前端 | RESTful | 标准 HTTP 协议 |

## 配置说明

### 后台刷盘参数

在 [src/main.rs](src/main.rs) 中配置：

```rust
// 每 5ms 或累积 1000 条触发一次刷盘
store.start_flusher(5, 1000);
```

- `interval_ms`：刷盘间隔（毫秒），越小延迟越低、CPU 越高
- `threshold`：待刷盘操作数阈值（预留参数）

### gRPC 消息大小

在 [src/main.rs](src/main.rs) 和 [client/src/grpc_client.rs](client/src/grpc_client.rs) 中配置：

```rust
.max_decoding_message_size(256 * 1024 * 1024)  // 256MB
.max_encoding_message_size(256 * 1024 * 1024)
```

### 读缓存大小

在 [src/main.rs](src/main.rs) 中配置：

```rust
let store = Store::open("data/kv.db", "data/meta.db", 10000)?;
//                                                   ↑ 缓存条目数
```

## 数据持久化

- `data/kv.db`：jammdb KV 数据（B+树，mmap）
- `data/meta.db`：SQLite 元数据（WAL 模式）
- `data/meta.db-wal`：SQLite WAL 日志
- `data/meta.db-shm`：SQLite 共享内存

崩溃恢复：jammdb 和 SQLite 均保证 ACID，重启后自动恢复到一致状态。未刷盘的 pending 数据会丢失（最多 5ms）。

## 开发

### 重新生成 protobuf 代码

修改 `proto/store.proto` 后，`cargo build` 会自动通过 `build.rs` 重新生成。

### 运行测试

```bash
# 启动服务
cargo run

# 另开终端运行客户端测试
cd client && cargo run
```

### 清理数据

```bash
rm -rf data/
```

## 依赖说明

| 依赖 | 版本 | 用途 |
|------|------|------|
| jammdb | 0.11 | bbolt 的 Rust 移植，B+树 KV |
| rusqlite | 0.29 | SQLite 绑定（bundled） |
| tonic | 0.9 | gRPC 框架 |
| warp | 0.3 | RESTful 框架 |
| tokio | 1.36 | 异步运行时 |
| dashmap | 5.4 | 线程安全缓存 |
| bytes | 1.0 | 零拷贝字节处理 |
| chrono | 0.4 | 时间处理 |

## 许可证

私有项目，未开源。
