// ============================================================
// Master Admin API 客户端
// ============================================================

import type {
  ApiResponse,
  ClusterOverview,
  WorkerNodeInfo,
  LogQueryResult,
  LogEntry,
  LogStats,
  RouteRule,
  HealthStatus,
} from '@/types';

const API_BASE = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:50052';
const WS_BASE = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:50053';

async function fetchApi<T>(path: string, options?: RequestInit): Promise<T> {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`API Error ${res.status}: ${err}`);
  }
  return res.json();
}

// ---------- 集群概览 ----------

export async function getOverview(): Promise<ApiResponse<ClusterOverview>> {
  return fetchApi('/api/v1/overview');
}

// ---------- Worker 管理 ----------

export async function getWorkers(alive?: boolean): Promise<ApiResponse<WorkerNodeInfo[]>> {
  const query = alive !== undefined ? `?alive=${alive}` : '';
  return fetchApi(`/api/v1/workers${query}`);
}

export async function getWorkerDetail(workerId: string): Promise<ApiResponse<WorkerNodeInfo>> {
  return fetchApi(`/api/v1/workers/${encodeURIComponent(workerId)}`);
}

// ---------- 日志管理 ----------

export interface LogQueryParams {
  worker_id?: string;
  level?: string;
  category?: string;
  keyword?: string;
  unread_only?: boolean;
  start_time?: string;
  end_time?: string;
  limit?: number;
  offset?: number;
}

export async function queryLogs(params: LogQueryParams): Promise<{
  success: boolean;
  data?: LogQueryResult;
  error?: string;
}> {
  const searchParams = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== '') {
      searchParams.set(k, String(v));
    }
  });
  return fetchApi(`/api/v1/logs?${searchParams.toString()}`);
}

export async function getLogStats(): Promise<ApiResponse<LogStats>> {
  return fetchApi('/api/v1/logs/stats');
}

export async function getRecentErrors(limit = 50): Promise<ApiResponse<LogEntry[]>> {
  return fetchApi(`/api/v1/logs/errors?limit=${limit}`);
}

export async function acknowledgeLog(logId: number): Promise<ApiResponse<boolean>> {
  return fetchApi(`/api/v1/logs/${logId}/ack`, { method: 'POST' });
}

export async function acknowledgeAllLogs(): Promise<ApiResponse<number>> {
  return fetchApi('/api/v1/logs/ack-all', { method: 'POST' });
}

// ---------- 路由规则 ----------

export async function getRoutes(): Promise<ApiResponse<RouteRule[]>> {
  return fetchApi('/api/v1/routes');
}

// ---------- 健康检查 ----------

export async function getHealth(): Promise<ApiResponse<HealthStatus>> {
  return fetchApi('/api/v1/health');
}

// ---------- 配置管理 ----------

export async function getConfig(): Promise<ApiResponse<import('@/types').AllConfigs>> {
  return fetchApi('/api/v1/config');
}

export async function updateConfig(config: import('@/types').AllConfigs): Promise<ApiResponse<{updated: number}>> {
  return fetchApi('/api/v1/config', {
    method: 'PUT',
    body: JSON.stringify(config),
  });
}

// ---------- Pending 缓存 ----------

export async function getPendingStats(): Promise<ApiResponse<import('@/types').PendingStats>> {
  return fetchApi('/api/v1/pending');
}

export async function clearPendingRegion(region: string): Promise<ApiResponse<{cleaned: number; region: string}>> {
  return fetchApi(`/api/v1/pending/${encodeURIComponent(region)}`, {
    method: 'DELETE',
  });
}

// ---------- WebSocket 连接 ----------

export function createLogWebSocket(): WebSocket {
  const ws = new WebSocket(`${WS_BASE}/api/v1/ws/logs`);
  return ws;
}
