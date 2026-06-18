// ============================================================
// 集群状态管理 (Zustand)
// ============================================================

import { create } from 'zustand';
import type {
  ClusterOverview,
  WorkerNodeInfo,
  LogEntry,
  LogStats,
  RouteRule,
} from '@/types';
import * as api from '@/lib/api';

interface ClusterState {
  // 集群概览
  overview: ClusterOverview | null;
  overviewLoading: boolean;
  overviewError: string | null;

  // Worker 列表
  workers: WorkerNodeInfo[];
  workersLoading: boolean;
  workersError: string | null;

  // 日志
  logs: LogEntry[];
  logTotal: number;
  logsLoading: boolean;
  logsError: string | null;
  logStats: LogStats | null;

  // 路由规则
  routes: RouteRule[];
  routesLoading: boolean;

  // WebSocket
  wsConnected: boolean;

  // 自动刷新
  autoRefresh: boolean;

  // Actions
  fetchOverview: () => Promise<void>;
  fetchWorkers: () => Promise<void>;
  fetchLogs: (params?: api.LogQueryParams) => Promise<void>;
  fetchLogStats: () => Promise<void>;
  fetchRoutes: () => Promise<void>;
  acknowledgeLog: (id: number) => Promise<void>;
  acknowledgeAllLogs: () => Promise<void>;
  setAutoRefresh: (v: boolean) => void;
  setWsConnected: (v: boolean) => void;
  addLogs: (entries: LogEntry[]) => void;
  updateWorkersFromWs: (workers: any[]) => void;
}

export const useClusterStore = create<ClusterState>((set, get) => ({
  overview: null,
  overviewLoading: false,
  overviewError: null,

  workers: [],
  workersLoading: false,
  workersError: null,

  logs: [],
  logTotal: 0,
  logsLoading: false,
  logsError: null,
  logStats: null,

  routes: [],
  routesLoading: false,

  wsConnected: false,
  autoRefresh: true,

  fetchOverview: async () => {
    set({ overviewLoading: true, overviewError: null });
    try {
      const res = await api.getOverview();
      if (res.success && res.data) {
        set({ overview: res.data });
      } else {
        set({ overviewError: res.error || '获取概览失败' });
      }
    } catch (e: any) {
      set({ overviewError: e.message });
    } finally {
      set({ overviewLoading: false });
    }
  },

  fetchWorkers: async () => {
    set({ workersLoading: true, workersError: null });
    try {
      const res = await api.getWorkers();
      if (res.success && res.data) {
        set({ workers: res.data });
      } else {
        set({ workersError: res.error || '获取 Worker 列表失败' });
      }
    } catch (e: any) {
      set({ workersError: e.message });
    } finally {
      set({ workersLoading: false });
    }
  },

  fetchLogs: async (params) => {
    set({ logsLoading: true, logsError: null });
    try {
      const res = await api.queryLogs(params || {});
      if (res.success && res.data) {
        set({ logs: res.data.entries, logTotal: res.data.total });
      } else {
        set({ logsError: res.error || '获取日志失败' });
      }
    } catch (e: any) {
      set({ logsError: e.message });
    } finally {
      set({ logsLoading: false });
    }
  },

  fetchLogStats: async () => {
    try {
      const res = await api.getLogStats();
      if (res.success && res.data) {
        set({ logStats: res.data });
      }
    } catch {
      // ignore
    }
  },

  fetchRoutes: async () => {
    set({ routesLoading: true });
    try {
      const res = await api.getRoutes();
      if (res.success && res.data) {
        set({ routes: res.data });
      }
    } catch {
      // ignore
    } finally {
      set({ routesLoading: false });
    }
  },

  acknowledgeLog: async (id: number) => {
    try {
      await api.acknowledgeLog(id);
      set((state) => ({
        logs: state.logs.map((l) =>
          l.id === id ? { ...l, acknowledged: true } : l
        ),
      }));
    } catch {
      // ignore
    }
  },

  acknowledgeAllLogs: async () => {
    try {
      await api.acknowledgeAllLogs();
      set((state) => ({
        logs: state.logs.map((l) => ({ ...l, acknowledged: true })),
      }));
    } catch {
      // ignore
    }
  },

  setAutoRefresh: (v) => set({ autoRefresh: v }),
  setWsConnected: (v) => set({ wsConnected: v }),

  addLogs: (entries) => {
    set((state) => {
      const existingIds = new Set(state.logs.map((l) => l.id));
      const newEntries = entries.filter((e) => !existingIds.has(e.id));
      return {
        logs: [...newEntries, ...state.logs].slice(0, 500),
        logTotal: state.logTotal + newEntries.length,
      };
    });
  },

  updateWorkersFromWs: (workers) => {
    set((state) => {
      // 检测是否有新上线的 worker（WS 推送中有但本地列表没有的）
      const existingIds = new Set(state.workers.map((w) => w.worker_id));
      const hasNewWorker = workers.some(
        (ww: any) => !existingIds.has(ww.worker_id)
      );

      // 检测本地是否有已离线但 WS 仍推送的 worker（数量不一致也触发刷新）
      if (hasNewWorker || workers.length !== state.workers.length) {
        // 触发一次完整刷新（异步，不阻塞当前更新）
        setTimeout(() => get().fetchWorkers(), 0);
      }

      const updated = state.workers.map((w) => {
        const wsWorker = workers.find((ww: any) => ww.worker_id === w.worker_id);
        if (wsWorker) {
          return {
            ...w,
            alive: wsWorker.alive,
            cpu_usage_ratio: wsWorker.cpu_usage_ratio,
            memory_usage_ratio: wsWorker.memory_usage_ratio,
            storage_usage_ratio: wsWorker.storage_usage_ratio,
            disk_health: wsWorker.disk_health,
          };
        }
        return w;
      });
      return { workers: updated };
    });
  },
}));
