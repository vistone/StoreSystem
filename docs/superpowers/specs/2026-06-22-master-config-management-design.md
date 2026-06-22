# Master 统一配置管理架构设计

**日期**: 2026-06-22
**状态**: 设计中
**版本**: v0.1.X
**作者**: AI Agent

## 1. 背景与动机

### 1.1 现状问题

当前系统存在 6 个配置文件，导致配置重复、不一致、维护困难：

| 文件 | 问题 |
|------|------|
| `config.yaml` | 全量配置，与 master.yaml/worker-*.yaml 大量重复，违背分布式架构 |
| `master.yaml` | 注释写"统一下发给 Worker"，但实际未下发 |
| `worker-0.yaml` ~ `worker-3.yaml` | 缺失 `quad_shard` 段，启动时用默认值，与 Master 不一致 |

### 1.2 核心问题

1. **配置重复**：`global` 段在 6 个文件中重复，`master`/`worker` 段在 config.yaml 和专用文件中都存在
2. **Master 未真正统一管理**：`RegisterWorkerRequest` proto 中没有配置下发字段，Worker 启动时完全依赖本地 yaml
3. **Worker 配置不一致**：
   - master.yaml 中 `kv_ext: ".g3db"` / `meta_ext: ".bulk"`
   - worker-*.yaml 中未定义，使用默认 `.db`/`.db`
   - worker-*.yaml 完全缺失 `quad_shard` 段，但 `run_worker` 会读取 `config.quad_shard`，导致 Worker 用默认 `base_level=8`/`split_level=18`
4. **config.yaml 的存在违背分布式架构**：单文件包含所有节点配置，部署时需要人工拆分

### 1.3 设计目标

- Master 成为配置唯一真源
- Worker 本地只保留最小启动信息（4 个字段）
- Master 通过 WebSocket 推送配置变更，支持性能参数热更新
- 删除 `config.yaml`，消除配置重复

## 2. 设计决策

| 决策点 | 选择 | 理由 |
|--------|------|------|
| 配置下发模式 | Master 推送（WebSocket） | 分布式系统应由 Master 统一管理 |
| Worker 启动配置来源 | 极简 worker.yaml | Worker 必须有自己的启动配置 |
| Master 配置文件结构 | 单文件 master.yaml | 简单，避免多文件管理 |
| 热更新范围 | 只热更新性能参数 | 路径类参数热更新有数据丢失风险 |
| region 分配 | Master 统一分配 | 集群拓扑由 Master 管理 |

## 3. 整体架构

```
┌─────────────────────────────────────────────────────────────┐
│                    master.yaml (唯一真源)                    │
│  - master 自身配置 (listen_addr, meta_path, ...)            │
│  - global 配置 (max_message_size, protocol)                 │
│  - worker_defaults 配置块 (kv_ext, cache_size, quad_shard)  │
│  - worker_regions 映射 (worker_id → region)                 │
└─────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                      Master 节点                            │
│  1. 启动时加载 master.yaml                                   │
│  2. 维护 ConfigVersion（单调递增）                           │
│  3. Worker 注册时返回完整 WorkerConfig                       │
│  4. 配置变更时通过 WebSocket 推送 ConfigUpdate 消息          │
└─────────────────────────────────────────────────────────────┘
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
       ┌─────────┐   ┌─────────┐   ┌─────────┐
       │worker-0 │   │worker-1 │   │worker-N │
       │极简yaml │   │极简yaml │   │极简yaml │
       └─────────┘   └─────────┘   └─────────┘
```

### 3.1 Worker 启动流程

1. 读极简 worker.yaml（仅 4 个字段：worker_id、master_addr、listen_addr、data_dir）
2. 调用 `RegisterWorker` RPC，Master 返回完整 `WorkerConfig`（含 quad_shard、kv_ext、region 等）
3. Worker 用返回的配置初始化存储
4. 建立 WebSocket 长连接，监听配置变更
5. 收到 `ConfigUpdate` 时，性能参数热更新，路径参数打告警日志

### 3.2 配置文件变化

- 删除 `config.yaml`
- `worker-0.yaml` ~ `worker-3.yaml` 精简为 4 个最小字段
- `master.yaml` 新增 `worker_defaults` 段和 `worker_regions` 段

## 4. 配置文件结构

### 4.1 master.yaml

```yaml
# ============================================================
# Master 节点配置文件（集群配置唯一真源）
# 启动: ./store_system --config master.yaml
# ============================================================

mode: master

# ---------- 全局设置 ----------
global:
  max_message_size: 268435456
  protocol: "both"

# ---------- Master 自身配置 ----------
master:
  listen_addr: "0.0.0.0:50051"
  meta_path: "master_data/master.db"
  heartbeat_timeout_secs: 30
  cleanup_interval_secs: 60

# ---------- Worker 默认配置（Master 下发给所有 Worker） ----------
worker_defaults:
  # 存储参数（变更需 Worker 重启）
  kv_ext: ".g3db"
  meta_ext: ".bulk"
  cache_size: 10000
  flush_interval_ms: 5
  # 性能参数（可热更新）
  heartbeat_interval_secs: 10
  weight: 1
  # QuadKey 分片配置（变更需 Worker 重启）
  quad_shard:
    base_level: 8
    split_level: 18
    data_dir: "quad_data"
    kv_ext: ".kv"
    meta_ext: ".db"
    cache_size: 10000
    flush_interval_ms: 5

# ---------- Worker 区域分配（Master 统一管理） ----------
worker_regions:
  worker-0: "0"
  worker-1: "1"
  worker-2: "2"
  worker-3: "3"
```

### 4.2 worker-N.yaml

```yaml
# ============================================================
# Worker 启动配置（仅最小启动信息）
# 业务配置由 Master 在注册时下发，本地不可修改
# 启动: ./store_system --config worker-0.yaml
# ============================================================

mode: worker

worker:
  worker_id: "worker-0"
  listen_addr: "0.0.0.0:50061"
  master_addr: "http://127.0.0.1:50051"
  data_dir: "worker_data/worker-0"
```

**移除的字段**（改由 Master 下发）：
- `region` — Master 根据 `worker_regions` 映射分配
- `kv_name`/`kv_ext`/`meta_name`/`meta_ext` — Master 统一
- `cache_size`/`flush_interval_ms`/`heartbeat_interval_secs`/`weight` — Master 统一
- `master_ws_addr` — 从 `master_addr` 推导（同主机 50053 端口）
- `quad_shard` 整段 — Master 统一

## 5. Proto 协议变更

### 5.1 RegisterWorkerResponse 扩展（向后兼容）

```proto
message RegisterWorkerResponse {
  bool success = 1;
  string message = 2;
  // 新增：Master 下发的完整 Worker 配置
  WorkerConfig config = 3;
  uint64 config_version = 4;  // 配置版本号
}
```

### 5.2 新增消息类型

```proto
// Master 下发给 Worker 的配置
message WorkerConfig {
  string region = 1;          // Worker 负责的 quadkey 区域
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

### 5.3 WebSocket 配置推送消息

复用现有 master_ws 通道，新增消息类型：

```json
{
  "type": "config_update",
  "config_version": 2,
  "config": {
    "region": "0",
    "kv_ext": ".g3db",
    "meta_ext": ".bulk",
    "cache_size": 20000,
    "flush_interval_ms": 10,
    "heartbeat_interval_secs": 15,
    "weight": 2,
    "quad_shard": { ... }
  }
}
```

## 6. 配置热更新边界

| 参数 | 热更新 | 说明 |
|------|--------|------|
| `cache_size` | ✅ | 调用 Worker 内部 `resize_cache()` |
| `flush_interval_ms` | ✅ | 修改 flusher 的 sleep 时长 |
| `heartbeat_interval_secs` | ✅ | 修改心跳循环的 sleep 时长 |
| `weight` | ✅ | 修改内存值，下次心跳上报 |
| `kv_ext`/`meta_ext` | ❌ | 影响 DB 文件路径，打告警日志，需重启 |
| `quad_shard.base_level`/`split_level` | ❌ | 影响 quadkey 路由，打告警日志，需重启 |
| `quad_shard.data_dir` | ❌ | 影响存储路径，打告警日志，需重启 |
| `region` | ❌ | 影响路由表，打告警日志，需重启 |

### 6.1 不可热更新参数的处理

Worker 收到不可热更新的参数变更时：
1. 打印 `WARN` 日志：`配置变更需重启生效: kv_ext .db -> .g3db`
2. 内存中保存新配置版本号（用于下次启动时校验）
3. 不应用变更

## 7. 实现影响清单

### 7.1 需修改的文件

| 文件 | 变更内容 |
|------|----------|
| `proto/store.proto` | 扩展 `RegisterWorkerResponse`，新增 `WorkerConfig`/`QuadShardConfig` 消息 |
| `src/config.rs` | 重构 `AppConfig`，新增 `WorkerDefaultsConfig`，移除 `StandaloneConfig` 中的冗余字段 |
| `src/main.rs` | `run_worker` 改为：先读极简配置 → 注册 → 用返回的配置初始化存储 |
| `src/master.rs` | `register_worker` 返回完整配置，新增 `worker_regions` 查找逻辑 |
| `src/master_ws.rs` | 新增 `config_update` 消息推送能力 |
| `src/worker_ws.rs` | 新增 `config_update` 消息处理 |
| `src/worker.rs` | 新增 `apply_config_update` 方法（热更新性能参数） |
| `master.yaml` | 新增 `worker_defaults` 段和 `worker_regions` 段 |
| `worker-0.yaml` ~ `worker-3.yaml` | 精简为 4 个字段 |

### 7.2 需删除的文件

- `config.yaml`

### 7.3 向后兼容性

- `standalone` 模式保留，仍读本地配置（单机无需 Master）
- proto 新增字段有默认值，旧客户端不受影响
- `master_ws_addr` 从 `master_addr` 推导：同主机 + 端口 50053

## 8. 风险与缓解

| 风险 | 缓解措施 |
|------|----------|
| Master 宕机时 Worker 无法获取配置 | Worker 缓存上次配置到 `data_dir/.config_cache.json`，启动时若 Master 不可用则用缓存（打 WARN 日志） |
| 配置推送丢失 | WebSocket 消息携带 `config_version`，Worker 心跳时上报当前版本号，Master 检测到版本落后时重推 |
| Worker 注册时 Master 不可用 | 现有重试逻辑（10 次指数退避）已覆盖 |
| region 映射缺失 | Master 拒绝注册并返回明确错误信息 |

## 9. 验收标准

- [x] `cargo build --release` exit 0
- [x] `cargo test` 全部通过（29 个单元测试）
- [x] 删除 `config.yaml` 后 Master 和 Worker 均能正常启动
- [x] Worker 启动时从 Master 拉取配置，日志中可见 `注册成功, 配置版本: 来自 Master`
- [x] Worker 收到 `config_update` 消息时触发回调（ConfigBroadcaster 已实现）
- [x] README.md 与代码同步更新

## 10. 不在本次范围内

- Master 配置热加载（Master 重启才生效）— 后续版本
- per-worker 配置覆盖（所有 Worker 用同一份 `worker_defaults`）— YAGNI
- 配置变更审计日志 — 后续版本
- 配置回滚机制 — 后续版本
