# Admin UI v0.2.0 — 全配置热操作 + 全工作流可视化

> 状态: 设计完成，待实现
> 日期: 2026-06-23
> 依赖: v0.1.8 (Master Pending Cache + StoreGuardian)

## 动机

当前 Admin UI 功能有限：
- **Settings 面板**仅有自动刷新开关和 API 地址显示，无法操作任何后端配置
- **Workflow 面板**只展示静态拓扑（Master → Worker → Storage），不反映运行时数据流方向、故障降级路径、Pending 缓存状态
- 缺乏 Pending 缓存管理界面
- 缺乏 Guardian 守护进程状态监控

本设计将 Admin UI 升级为全功能运维控制台：所有后端配置项均可热操作，所有运行时工作流均可视化。

## 架构

```
┌──────────────────────────────────────────────────────┐
│                   Admin UI (port 3000)                │
│                                                      │
│  ┌─────────┐ ┌──────────┐ ┌────────┐ ┌───────────┐ │
│  │Workflow │ │ Settings │ │Pending │ │ Log/Route │ │
│  │ Panel   │ │ Panel    │ │ Panel  │ │ (existing)│ │
│  └────┬────┘ └────┬─────┘ └───┬────┘ └───────────┘ │
└───────┼───────────┼──────────┼───────────────────────┘
        │           │          │
   ┌────▼───────────▼──────────▼───────────────────────┐
   │          Master Admin API (port 50052)             │
   │                                                   │
   │  GET /api/v1/config           → 列出所有配置       │
   │  PUT /api/v1/config           → 更新配置（批量）   │
   │  GET /api/v1/config/yaml      → 导出配置为 YAML    │
   │  POST /api/v1/config/yaml     → 从 YAML 导入       │
   │  GET /api/v1/pending          → 各 region pending  │
   │  DELETE /api/v1/pending/:region → 清空 pending    │
   │  PUT /api/v1/workers/:id/config → 更新 Worker 配置 │
   │  WS /api/v1/ws/workflow       → 实时工作流数据推送 │
   └──────────────────┬────────────────────────────────┘
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
      Master       Worker      Pending
      Config       Config      Store
      (本地热更新) (WS推送热更新) (GC/清理)
```

## Settings Panel — 配置表单

### 布局

左侧树形导航 + 右侧分组表单：

```
┌──────────────┬────────────────────────────────────────┐
│ 配置分组      │  配置表单                               │
│              │                                        │
│ ▶ Master     │  ┌─ Master 集群参数 ──────────────────┐ │
│   Worker     │  │ heartbeat_timeout_secs    [30] 秒   │ │
│   Pending    │  │ cleanup_interval_secs     [60] 秒   │ │
│   Guardian   │  │ max_message_size         [256] MB   │ │
│   Replica    │  │ protocol         [grpc/restful/ws]  │ │
│   QuadKey    │  └────────────────────────────────────┘ │
│              │                                        │
│              │  [保存] [重置] [导出 YAML] [导入 YAML]   │
└──────────────┴────────────────────────────────────────┘
```

### 配置字段清单

| 分组 | 字段 | 类型 | 生效方式 |
|------|------|------|---------|
| **Master** | heartbeat_timeout_secs | number | Master 本地热更新 |
| | cleanup_interval_secs | number | Master 本地 |
| | max_message_size | number (MB) | Master 本地 |
| | protocol | select | 需重启 ⚠️ |
| **Worker** | cache_size | number | WS 推送热更新 |
| | flush_interval_ms | number | WS 推送热更新 |
| | heartbeat_interval_secs | number | WS 推送热更新 |
| | weight | number | WS 推送热更新 |
| | kv_ext | text | 需重启 ⚠️ |
| | meta_ext | text | 需重启 ⚠️ |
| **Pending** | gc_interval_secs | number | Master 本地热更新 |
| | flush_timeout_secs | number | Master 本地热更新 |
| **Guardian** | probe_interval_secs | number | YAML 回写 |
| | probe_timeout_secs | number | YAML 回写 |
| | failure_threshold | number | YAML 回写 |
| | backoff_base_secs | number | YAML 回写 |
| | backoff_max_secs | number | YAML 回写 |
| | cooldown_after_failures | number | YAML 回写 |
| | cooldown_secs | number | YAML 回写 |
| **Replica** | replication_factor | number | MasterStore 落盘 |
| | strategy | select | MasterStore 落盘 |
| **QuadKey** | base_level | number | MasterStore + WS 推 Worker |
| | split_level | number | MasterStore + WS 推 Worker |

### 交互

- 修改字段 → 边框高亮蓝色 → "保存"按钮激活
- 点击保存 → `PUT /api/v1/config` → 后端逐个应用 → toast
- 需重启的字段旁 ⚠️ 图标 + tooltip
- "导出 YAML" → `GET /api/v1/config/yaml` → 下载
- "导入 YAML" → 文件选择器 → `POST /api/v1/config/yaml` → diff → 确认

## Workflow Panel — 全数据流可视化

### 增强节点

```
                              ┌──────────────────┐
                              │    Guardian       │
                              │  PID: 12345       │
                              │  探针: ✅ 5s前     │
                              └───┬──────┬────┬───┘
                                  │      │    │
                    ┌─────────────┘      │    └─────────────┐
                    ▼                    ▼                  ▼
┌────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│ Client │───▶│ Master  │───▶│Worker-0 │───▶│Worker-1 │───▶│Worker-3 │
│        │    │ :50051  │    │ region0 │    │ region1 │    │ region3 │
│        │    │         │    │ ✅ 6K/s │    │ ✅ 6K/s │    │ ✅ 6K/s │
└────────┘    │ ┌─────┐ │    └─────────┘    └─────────┘    └─────────┘
              │ │Pend │ │         │               │               │
              │ │ing  │ │    ┌────▼───┐      ┌────▼───┐     ┌────▼───┐
              │ │reg2 │ │    │ Storage│      │ Storage│     │ Storage│
              │ │12MB │ │    │ 45%    │      │ 32%    │     │ 58%    │
              │ └─────┘ │    └────────┘      └────────┘     └────────┘
              └────┬─────┘
                   │
              ┌────▼─────┐
              │Worker-2  │ ← 宕机
              │ region2  │    虚线边框 + 红色
              │ ❌ DOWN  │
              └──────────┘
              Pending 替代路径（橙色虚线）:
              Master → PendingStore(region_2)
              Guardian → Worker-2 (蓝色点线，监控关系)
```

### 连线语义

| 连线 | 颜色 | 样式 | 含义 |
|------|------|------|------|
| Master → Worker | 绿色 | 实线 + 动画 | Worker 存活 |
| Master → Worker | 红色 | 实线 | 探针失败 |
| Master → Worker | 灰色 | 虚线 | Worker 宕机 |
| Master → Pending | 橙色 | 虚线 + 动画 | 降级写入 Pending |
| Guardian → 进程 | 蓝色 | 点线 | 守护监控关系 |

### 实时数据推送

新 WebSocket `/api/v1/ws/workflow`，每 2s 推送：

```json
{
  "type": "workflow",
  "master": { "alive": true },
  "guardian": { "alive": true, "restart_count": 0, "last_probe": "2s ago" },
  "workers": [
    {
      "worker_id": "worker-0", "alive": true, "region": "0",
      "ops_per_sec": 6145, "cpu": 0.23, "memory_ratio": 0.45,
      "storage_ratio": 0.32, "disk_health": "Healthy"
    }
  ],
  "pending": {
    "region_0": { "count": 0, "bytes": 0 },
    "region_1": { "count": 0, "bytes": 0 },
    "region_2": { "count": 5000, "bytes": 12582912 },
    "region_3": { "count": 0, "bytes": 0 }
  }
}
```

## Pending Panel — 缓存管理

新增独立面板：

```
┌────────────────────────────────────────────────┐
│  Pending 缓存管理                               │
│                                                │
│  Region 0: [▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓] 0 条 / 0B   │
│  Region 1: [▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓] 0 条 / 0B   │
│  Region 2: [████████░░░░░░░░░░░░] 5000 条       │
│            12.0 MB   写回中…     [清空]         │
│  Region 3: [▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓] 0 条 / 0B   │
│                                                │
│  自动刷新 [ON]  上次刷新: 2s 前                  │
└────────────────────────────────────────────────┘
```

## 后端 API 新增

### 配置管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/config` | 返回所有配置（分组 JSON） |
| `PUT` | `/api/v1/config` | 批量更新配置 |
| `GET` | `/api/v1/config/yaml` | 导出当前配置为 YAML |
| `POST` | `/api/v1/config/yaml` | 从 YAML 导入配置 |
| `PUT` | `/api/v1/workers/:id/config` | 更新特定 Worker，WS 推送 |

### Pending 管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/pending` | 各 region pending 统计 |
| `DELETE` | `/api/v1/pending/:region` | 手动清空某 region |

### WebSocket

| 路径 | 说明 |
|------|------|
| `WS /api/v1/ws/workflow` | 实时工作流数据推送（2s 间隔） |

## 实现清单

| 优先级 | 任务 | 文件 |
|:------:|------|------|
| **P0** | 后端: config API (GET/PUT/YAML) | `src/master_admin_http.rs` |
| **P0** | 后端: pending API (GET/DELETE) | `src/master_admin_http.rs` |
| **P0** | 后端: workflow WS 推送 | `src/master_admin_http.rs` |
| **P0** | 前端: ConfigPanel 全配置表单 | `admin-ui/src/components/panels/ConfigPanel.tsx` |
| **P0** | 前端: 增强 ClusterWorkflow（Guardian/Pending 节点 + 连线语义） | `admin-ui/src/components/workflow/ClusterWorkflow.tsx` |
| **P0** | 前端: PendingPanel | `admin-ui/src/components/panels/PendingPanel.tsx` |
| **P1** | 前端: 类型定义 + API 客户端更新 | `admin-ui/src/types/` + `admin-ui/src/lib/api.ts` |
| **P1** | 前端: Store 状态管理更新 | `admin-ui/src/stores/cluster-store.ts` |
| **P1** | 前端: Sidebar 增加 Pending 导航 | `admin-ui/src/components/ui/Sidebar.tsx` |
| **P2** | YAML 导入预览 diff | `admin-ui/src/components/panels/ConfigPanel.tsx` |

## 风险

| 风险 | 缓解 |
|------|------|
| 配置热更新部分项需重启 | ⚠️ 标记 + tooltip 明确告知 |
| Guardian 配置回写 YAML 可能冲突 | 加文件锁，写入前备份 |
| 配置项数量多，前端表单复杂 | 树形分组导航 + 搜索过滤 |
| WS workflow 数据量大 | 2s 间隔 + 增量推送（只推变化项） |
