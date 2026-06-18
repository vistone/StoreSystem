// ============================================================
// Worker 节点 - 工作流图中的 Worker 节点
// ============================================================

'use client';

import { memo } from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Cpu, Wifi, Activity } from 'lucide-react';
import type { WorkerNodeInfo } from '@/types';
import { formatPercent } from '@/lib/utils';

interface WorkerNodeData {
  worker: WorkerNodeInfo;
}

export const WorkerNode = memo(({ data }: NodeProps<WorkerNodeData>) => {
  const { worker } = data;
  const isAlive = worker.alive;

  return (
    <div
      className={`px-3 py-2.5 shadow-lg rounded-xl border-2 min-w-[180px] transition-all duration-300 ${
        isAlive
          ? 'border-green-400 bg-gradient-to-br from-green-50 to-emerald-100 dark:from-green-950 dark:to-emerald-900'
          : 'border-gray-400 bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-800 dark:to-gray-700 opacity-70'
      }`}
    >
      <Handle type="target" position={Position.Top} className="!bg-green-500" />
      <Handle type="source" position={Position.Bottom} className="!bg-blue-500" />

      <div className="flex items-center gap-2 mb-1.5">
        <div
          className={`w-2.5 h-2.5 rounded-full ${
            isAlive ? 'bg-green-500 animate-pulse' : 'bg-gray-400'
          }`}
        />
        <span className="font-semibold text-sm text-gray-800 dark:text-gray-200">
          {worker.worker_id}
        </span>
        <span className="text-[10px] text-gray-400 ml-auto">{worker.address}</span>
      </div>

      <div className="grid grid-cols-2 gap-x-3 gap-y-1 text-[11px]">
        <div className="flex items-center gap-1">
          <Cpu className="w-3 h-3 text-blue-500" />
          <span className="text-gray-600 dark:text-gray-400">
            CPU: {formatPercent(worker.cpu_usage_ratio)}
          </span>
        </div>
        <div className="flex items-center gap-1">
          <Activity className="w-3 h-3 text-purple-500" />
          <span className="text-gray-600 dark:text-gray-400">
            内存: {formatPercent(worker.memory_usage_ratio)}
          </span>
        </div>
        <div className="flex items-center gap-1">
          <Wifi className="w-3 h-3 text-cyan-500" />
          <span className="text-gray-600 dark:text-gray-400">
            连接: {worker.active_connections}
          </span>
        </div>
        <div className="flex items-center gap-1">
          <span className="text-gray-600 dark:text-gray-400">
            权重: {worker.weight}
          </span>
        </div>
      </div>

      {/* 健康状态条 */}
      <div className="mt-1.5 flex gap-1">
        <div className="flex-1 h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{
              width: `${Math.min(worker.cpu_usage_ratio * 100, 100)}%`,
              backgroundColor:
                worker.cpu_usage_ratio > 0.8
                  ? '#ef4444'
                  : worker.cpu_usage_ratio > 0.6
                  ? '#eab308'
                  : '#22c55e',
            }}
          />
        </div>
        <div className="flex-1 h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{
              width: `${Math.min(worker.memory_usage_ratio * 100, 100)}%`,
              backgroundColor:
                worker.memory_usage_ratio > 0.8
                  ? '#ef4444'
                  : worker.memory_usage_ratio > 0.6
                  ? '#eab308'
                  : '#22c55e',
            }}
          />
        </div>
        <div className="flex-1 h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{
              width: `${Math.min(worker.storage_usage_ratio * 100, 100)}%`,
              backgroundColor:
                worker.storage_usage_ratio > 0.8
                  ? '#ef4444'
                  : worker.storage_usage_ratio > 0.6
                  ? '#eab308'
                  : '#22c55e',
            }}
          />
        </div>
      </div>
      <div className="flex justify-between text-[9px] text-gray-400 mt-0.5">
        <span>CPU</span>
        <span>内存</span>
        <span>存储</span>
      </div>
    </div>
  );
});

WorkerNode.displayName = 'WorkerNode';
