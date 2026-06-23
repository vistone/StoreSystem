# StoreGuardian — 进程守护者设计

> 状态: 设计完成，待实现
> 日期: 2026-06-23
> 补充: 进程存活性监控 + 假死检测 + 快速自愈

## 动机

当前系统无进程级守护机制：
- Master/Worker 进程崩溃后无自动拉起
- 进程假死（PID 存在但不响应）无法检测
- 依赖人工干预恢复，不符合生产要求

本设计提供一个独立的外部进程守护者 `store_guardian`，fork/exec 管理 Master 和 Worker 进程，通过深度探针检测假死，支持指数退避 + 冷却策略的自动重启。

## 架构

```
┌─────────────────────────────────────────────┐
│              store_guardian                  │
│                                             │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │ Process  │  │ Health   │  │ Restart   │ │
│  │ Manager  │  │ Prober   │  │ Policy    │ │
│  │(fork/exec│  │(深度探针) │  │(退避+冷却)│ │
│  │ PID追踪) │  │Put+Get   │  │          │ │
│  └────┬─────┘  └────┬─────┘  └─────┬─────┘ │
└───────┼──────────────┼──────────────┼───────┘
        │              │              │
   ┌────▼────┐    ┌────▼────┐         │
   │ Master  │    │ Worker  │    restart
   │ :50051  │◄───│ :50061  │◄────────┘
   └─────────┘    └─────────┘
```

## 进程状态模型

```rust
struct GuardedProcess {
    name: String,           // "master" | "worker-0" | ...
    config: ProcessConfig,  // path, args, health endpoints
    pid: Option<u32>,
    state: ProcessState,
    failures: u32,
    last_failure: Instant,
    cooldown_until: Option<Instant>,
}

enum ProcessState {
    Starting,    // 刚启动，等首次探针通过
    Running,     // 健康
    Degraded,    // 探针部分失败
    Dead,        // 探针全部失败或进程不存在
    Cooldown,    // 冷却中，不重启
}
```

状态流转：

```
Starting ──探针通过──▶ Running ──探针失败──▶ Degraded ──连续N次──▶ Dead
                           ▲                      │                 │
                           │                      │ 探针恢复        │ kill -9 + 重启
                           │                      ▼                 ▼
                           └────────────────── Running          Starting
                                                                     │
                              Cooldown ◀── 连续M次失败 ── Dead ─────┘
                                 │
                                 │ 冷却结束
                                 ▼
                              Starting (重置 failures=0)
```

每个进程独立状态机，互不影响。`depends_on` 仅在首次启动时生效。

## 深度探针

探针间隔到达后，对每个进程执行 gRPC Put+Get 往返：

```
探测 Master（StoreService, :50051）:
  quadkey = "0"（固定区域）
  Put(key="__health__", value=timestamp_bytes, quadkey="0", level=0)
  Get(key="__health__", quadkey="0", level=0)
  → value == timestamp_bytes ? Running : Degraded

探测 Worker（WorkerService, :5006X）:
  同上，直接调用 Worker 的 WorkerService 接口
```

探测结果分类：

| 结果 | 含义 | 动作 |
|------|------|------|
| `Running` | Put+Get 都成功，数据一致 | failures=0 |
| `Degraded` | Put 成功但 Get 失败/数据不一致 | failures+=1 |
| `Timeout` | 连接超时 | failures+=1 |
| `ConnectionRefused` | 端口拒绝连接 | failures+=1 |
| `ProcessGone` | PID 不存在 | 直接 Dead |

**探测循环**（每个进程独立 tokio task）：

```
loop:
  sleep(probe_interval)
  
  检查 PID → 不存在 → Dead → handle_dead
  
  执行深度探针:
    Running  → failures=0
    失败     → failures+=1, Degraded
    failures>=threshold → Dead → handle_dead
```

**Depends_on 处理**：master 探测失败时，所有 `depends_on: master` 的 Worker 跳过探测（保留当前状态），等 master 恢复后再恢复探测。

## 重启策略

```
kill -9 旧进程
  → 退避等待: min(backoff_base × 2^(failures-1), backoff_max)
  → fork+exec 重启
  → 状态 = Starting

连续失败上限 → 冷却期（cooldown_secs）
  → 冷却期内不重启
  → 冷却结束 → 重置 failures=0，重新开始
```

示例（backoff_base=1s, backoff_max=60s, cooldown_after=10, cooldown=300s）：

| failures | 等待 | 动作 |
|----------|------|------|
| 3 | 1s | 重启 |
| 4 | 2s | 重启 |
| 5 | 4s | 重启 |
| 6 | 8s | 重启 |
| 7 | 16s | 重启 |
| 8 | 32s | 重启 |
| 9 | 60s | 重启 |
| 10 | — | 进入冷却 300s |

## 配置（guardian.yaml）

```yaml
guardian:
  probe_interval_secs: 5
  probe_timeout_secs: 3
  failure_threshold: 3
  backoff_base_secs: 1
  backoff_max_secs: 60
  cooldown_after_failures: 10
  cooldown_secs: 300

processes:
  master:
    path: "./bin/store_system"
    args: ["--config", "master.yaml"]
    health_grpc: "127.0.0.1:50051"

  worker-0:
    path: "./bin/store_system"
    args: ["--config", "worker-0.yaml"]
    health_grpc: "127.0.0.1:50061"
    depends_on: "master"

  worker-1:
    path: "./bin/store_system"
    args: ["--config", "worker-1.yaml"]
    health_grpc: "127.0.0.1:50062"
    depends_on: "master"

  worker-2:
    path: "./bin/store_system"
    args: ["--config", "worker-2.yaml"]
    health_grpc: "127.0.0.1:50063"
    depends_on: "master"

  worker-3:
    path: "./bin/store_system"
    args: ["--config", "worker-3.yaml"]
    health_grpc: "127.0.0.1:50064"
    depends_on: "master"
```

## 启动顺序

```
1. 解析 guardian.yaml
2. 按 depends_on 拓扑排序（无依赖的先启动）
3. 先 fork+exec master
4. 循环探测 master 直到 Running
5. 逐个 fork+exec worker-0/1/2/3
6. 所有进程存活后进入持续监控循环
```

## CLI

```bash
# 管理所有进程
./store_guardian --config guardian.yaml

# 仅管理 Master
./store_guardian --config guardian.yaml --role master

# 仅管理 Worker
./store_guardian --config guardian.yaml --role worker
```

## 二进制结构

新建 `guardian/` crate（独立二进制，极简依赖）：

```
guardian/
├── Cargo.toml          # tonic + tokio + serde_yaml + chrono
└── src/
    ├── main.rs         # CLI 入口
    ├── config.rs       # GuardianConfig, ProcessConfig 解析
    ├── process.rs      # ProcessState, spawn, kill, PID 检测
    ├── prober.rs       # 深度探针（gRPC Put+Get 往返）
    └── policy.rs       # 退避+冷却策略
```

依赖：

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
tonic = "0.12"
prost = "0.13"
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
chrono = "0.4"
```

Proto 复用主项目的 `proto/store.proto`（通过 `build.rs` 编译）。

## 实现清单

| 优先级 | 任务 | 文件 |
|:------:|------|------|
| P0 | `GuardianConfig` / `ProcessConfig` 结构体 + YAML 解析 | `guardian/src/config.rs` |
| P0 | `ProcessManager`: fork/exec, PID 追踪, kill | `guardian/src/process.rs` |
| P0 | `Prober`: 深度探针 Put+Get 往返 | `guardian/src/prober.rs` |
| P0 | `RestartPolicy`: 退避 + 冷却 + 状态机 | `guardian/src/policy.rs` |
| P0 | `main.rs`: CLI, 启动顺序, 监控循环 | `guardian/src/main.rs` |
| P1 | 集成测试：杀 Worker → 检测 → 自动重启 | `guardian/tests/` |
| P2 | 日志/告警输出 | `guardian/src/main.rs` |

## 风险

| 风险 | 缓解 |
|------|------|
| 守护自身崩溃 | 进程极简（无复杂依赖），依赖 systemd 拉起守护 |
| 频繁重启风暴 | 冷却机制 + 退避上限 |
| 深度探针引入额外负载 | 每 5s 一条轻量 Put+Get（几十字节），可忽略 |
| 探针写脏数据 | key 前缀 `__health__` 与业务 key 无冲突 |
