// ============================================================
// 集群管理界面类型定义
// ============================================================

// ---------- API 响应 ----------

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
}

// ---------- 集群概览 ----------

export interface ClusterOverview {
  total_workers: number;
  alive_workers: number;
  dead_workers: number;
  total_storage_bytes: number;
  used_storage_bytes: number;
  total_memory_bytes: number;
  used_memory_bytes: number;
  avg_cpu_usage: number;
  total_logs_today: number;
  unread_logs: number;
  error_logs_today: number;
}

// ---------- Worker 节点 ----------

export interface WorkerNodeInfo {
  worker_id: string;
  address: string;
  weight: number;
  alive: boolean;
  last_heartbeat: number;
  storage_used_bytes: number;
  storage_capacity_bytes: number;
  storage_usage_ratio: number;
  disk_health: string;
  memory_used_bytes: number;
  memory_total_bytes: number;
  memory_usage_ratio: number;
  cpu_usage_ratio: number;
  cpu_cores: number;
  active_connections: number;
  tags: Record<string, string>;
  // ---- 写入统计（v0.3.0 新增） ----
  total_put_count: number;       // 累计写入操作数
  total_put_bytes: number;       // 累计写入字节数
  flushed_count: number;         // 已刷盘操作数（已入库）
  flushed_bytes: number;         // 已刷盘字节数（已入库）
  pending_count: number;         // 待刷盘操作数（缓存中未入库）
  pending_bytes: number;         // 待刷盘字节数（缓存中未入库）
  write_rate_per_sec: number;    // 写入速率 ops/sec
  write_bytes_per_sec: number;   // 写入带宽 bytes/sec
}

// ---------- 日志 ----------

export interface LogEntry {
  id: number;
  worker_id: string;
  level: string;
  category: string;
  message: string;
  detail_json?: string;
  timestamp: string;
  acknowledged: boolean;
}

export interface LogQueryResult {
  entries: LogEntry[];
  total: number;
  limit: number;
  offset: number;
}

export interface LogStats {
  total: number;
  unread: number;
  errors: number;
  today: number;
  by_worker: [string, number][];
}

// ---------- 路由规则 ----------

export interface RouteRule {
  key_prefix: string;
  worker_id: string;
  priority: number;
  created_at: string;
}

// ---------- 健康检查 ----------

export interface HealthStatus {
  status: string;
  alive_workers: number;
  total_workers: number;
  timestamp: string;
}

// ---------- WebSocket 消息 ----------

export interface WsLogMessage {
  type: 'logs';
  data: LogEntry[];
}

export interface WsOverviewMessage {
  type: 'overview';
  data: {
    total_workers: number;
    alive_workers: number;
    workers: {
      worker_id: string;
      alive: boolean;
      cpu_usage_ratio: number;
      memory_usage_ratio: number;
      storage_usage_ratio: number;
      disk_health: string;
    }[];
  };
}

export type WsMessage = WsLogMessage | WsOverviewMessage;

// ---------- 工作流节点类型 ----------

export type WorkflowNodeType =
  | 'master'
  | 'worker'
  | 'worker-alive'
  | 'worker-dead'
  | 'storage'
  | 'log'
  | 'route';

export interface WorkflowNodeData {
  label: string;
  type: WorkflowNodeType;
  worker?: WorkerNodeInfo;
  stats?: {
    cpu?: number;
    memory?: number;
    storage?: number;
    connections?: number;
  };
  status?: 'healthy' | 'warning' | 'critical' | 'dead';
}
