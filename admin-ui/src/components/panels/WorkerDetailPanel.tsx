// ============================================================
// Worker 详情面板 - 表格形式展示 Worker 详细信息
// ============================================================

"use client";

import { useEffect, useState } from "react";
import {
  Server,
  Cpu,
  HardDrive,
  Activity,
  Wifi,
  Clock,
  Tag,
  Search,
  RefreshCw,
} from "lucide-react";
import { useClusterStore } from "@/stores/cluster-store";
import {
  formatBytes,
  formatPercent,
  formatTimestamp,
  getDiskHealthColor,
} from "@/lib/utils";
import type { WorkerNodeInfo } from "@/types";

export function WorkerDetailPanel() {
  const { workers, workersLoading, fetchWorkers, autoRefresh } =
    useClusterStore();
  const [searchTerm, setSearchTerm] = useState("");
  const [sortBy, setSortBy] = useState<"cpu" | "memory" | "storage" | "name">(
    "name",
  );

  useEffect(() => {
    fetchWorkers();
    if (!autoRefresh) return;
    const interval = setInterval(fetchWorkers, 5000);
    return () => clearInterval(interval);
  }, [fetchWorkers, autoRefresh]);

  const filtered = workers
    .filter(
      (w) =>
        w.worker_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
        w.address.toLowerCase().includes(searchTerm.toLowerCase()),
    )
    .sort((a, b) => {
      switch (sortBy) {
        case "cpu":
          return b.cpu_usage_ratio - a.cpu_usage_ratio;
        case "memory":
          return b.memory_usage_ratio - a.memory_usage_ratio;
        case "storage":
          return b.storage_usage_ratio - a.storage_usage_ratio;
        default:
          return a.worker_id.localeCompare(b.worker_id);
      }
    });

  return (
    <div className="flex flex-col h-full bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700">
      {/* 标题栏 */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-100 dark:border-gray-700">
        <div className="flex items-center gap-2">
          <Server className="w-4 h-4 text-green-500" />
          <span className="font-semibold text-sm text-gray-700 dark:text-gray-300">
            Worker 节点
          </span>
          <span className="text-xs text-gray-400">({workers.length} 个)</span>
        </div>
        <div className="flex items-center gap-2">
          <div className="relative">
            <Search className="w-3 h-3 absolute left-2 top-1/2 -translate-y-1/2 text-gray-400" />
            <input
              type="text"
              placeholder="搜索 Worker..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="pl-7 pr-2 py-1 text-xs border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 w-40"
            />
          </div>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as any)}
            className="text-xs border border-gray-200 dark:border-gray-600 rounded-lg px-2 py-1 bg-white dark:bg-gray-800"
          >
            <option value="name">按名称</option>
            <option value="cpu">按 CPU</option>
            <option value="memory">按内存</option>
            <option value="storage">按存储</option>
          </select>
          <button
            onClick={() => fetchWorkers()}
            className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
          >
            <RefreshCw
              className={`w-3.5 h-3.5 ${workersLoading ? "animate-spin" : ""}`}
            />
          </button>
        </div>
      </div>

      {/* 表格 */}
      <div className="flex-1 overflow-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="bg-gray-50 dark:bg-gray-900/50 text-gray-500">
              <th className="text-left px-4 py-2 font-medium">Worker</th>
              <th className="text-left px-3 py-2 font-medium">地址</th>
              <th className="text-center px-3 py-2 font-medium">状态</th>
              <th className="text-center px-3 py-2 font-medium">CPU</th>
              <th className="text-center px-3 py-2 font-medium">内存</th>
              <th className="text-center px-3 py-2 font-medium">存储</th>
              <th className="text-center px-3 py-2 font-medium">磁盘</th>
              <th className="text-center px-3 py-2 font-medium">连接</th>
              <th className="text-right px-4 py-2 font-medium">心跳</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-50 dark:divide-gray-800">
            {filtered.map((worker) => (
              <WorkerRow key={worker.worker_id} worker={worker} />
            ))}
          </tbody>
        </table>
        {filtered.length === 0 && (
          <div className="flex items-center justify-center h-32 text-gray-400 text-sm">
            暂无 Worker 节点
          </div>
        )}
      </div>
    </div>
  );
}

function WorkerRow({ worker }: { worker: WorkerNodeInfo }) {
  const diskColor = getDiskHealthColor(worker.disk_health);

  return (
    <tr className="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
      <td className="px-4 py-2.5">
        <div className="flex items-center gap-2">
          <div
            className={`w-2 h-2 rounded-full ${
              worker.alive ? "bg-green-500" : "bg-gray-400"
            }`}
          />
          <span className="font-medium text-gray-800 dark:text-gray-200">
            {worker.worker_id}
          </span>
        </div>
      </td>
      <td className="px-3 py-2.5 text-gray-500 font-mono">{worker.address}</td>
      <td className="px-3 py-2.5 text-center">
        <span
          className={`px-2 py-0.5 rounded-full text-[10px] font-medium ${
            worker.alive
              ? "bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400"
              : "bg-gray-100 text-gray-500 dark:bg-gray-900/30 dark:text-gray-400"
          }`}
        >
          {worker.alive ? "在线" : "离线"}
        </span>
      </td>
      <td className="px-3 py-2.5">
        <div className="flex items-center gap-2">
          <div className="flex-1 h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full"
              style={{
                width: `${Math.min(worker.cpu_usage_ratio * 100, 100)}%`,
                backgroundColor:
                  worker.cpu_usage_ratio > 0.8
                    ? "#ef4444"
                    : worker.cpu_usage_ratio > 0.6
                      ? "#eab308"
                      : "#22c55e",
              }}
            />
          </div>
          <span className="text-gray-600 dark:text-gray-400 w-10 text-right">
            {formatPercent(worker.cpu_usage_ratio)}
          </span>
        </div>
      </td>
      <td className="px-3 py-2.5">
        <div className="flex items-center gap-2">
          <div className="flex-1 h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full"
              style={{
                width: `${Math.min(worker.memory_usage_ratio * 100, 100)}%`,
                backgroundColor:
                  worker.memory_usage_ratio > 0.8
                    ? "#ef4444"
                    : worker.memory_usage_ratio > 0.6
                      ? "#eab308"
                      : "#3b82f6",
              }}
            />
          </div>
          <span className="text-gray-600 dark:text-gray-400 w-10 text-right">
            {formatPercent(worker.memory_usage_ratio)}
          </span>
        </div>
      </td>
      <td className="px-3 py-2.5">
        <div className="flex items-center gap-2">
          <div className="flex-1 h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
            <div
              className="h-full rounded-full"
              style={{
                width: `${Math.min(worker.storage_usage_ratio * 100, 100)}%`,
                backgroundColor: diskColor,
              }}
            />
          </div>
          <span className="text-gray-600 dark:text-gray-400 w-10 text-right">
            {formatPercent(worker.storage_usage_ratio)}
          </span>
        </div>
      </td>
      <td className="px-3 py-2.5 text-center">
        <span
          className="px-1.5 py-0.5 rounded text-[10px] font-medium"
          style={{
            backgroundColor: `${diskColor}20`,
            color: diskColor,
          }}
        >
          {worker.disk_health}
        </span>
      </td>
      <td className="px-3 py-2.5 text-center text-gray-600 dark:text-gray-400">
        {worker.active_connections}
      </td>
      <td className="px-4 py-2.5 text-right text-gray-400 font-mono text-[10px]">
        {formatTimestamp(worker.last_heartbeat)}
      </td>
    </tr>
  );
}
