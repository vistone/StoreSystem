// ============================================================
// Master 节点 - 工作流图中的 Master 节点
// ============================================================

'use client';

import { memo } from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { Server, Activity, Users, HardDrive } from 'lucide-react';
import type { ClusterOverview } from '@/types';

interface MasterNodeData {
  overview?: ClusterOverview | null;
  workerCount: number;
  aliveCount: number;
}

export const MasterNode = memo(({ data }: NodeProps<MasterNodeData>) => {
  const { overview, workerCount, aliveCount } = data;

  return (
    <div className="px-4 py-3 shadow-xl rounded-xl border-2 border-indigo-500 bg-gradient-to-br from-indigo-50 to-indigo-100 dark:from-indigo-950 dark:to-indigo-900 min-w-[200px]">
      <Handle type="source" position={Position.Bottom} className="!bg-indigo-500" />

      <div className="flex items-center gap-2 mb-2">
        <Server className="w-5 h-5 text-indigo-600" />
        <span className="font-bold text-indigo-800 dark:text-indigo-200">Master 节点</span>
      </div>

      <div className="space-y-1.5 text-xs">
        <div className="flex items-center gap-2">
          <Users className="w-3.5 h-3.5 text-gray-500" />
          <span className="text-gray-600 dark:text-gray-400">
            Worker: <span className="font-semibold text-green-600">{aliveCount}</span>
            <span className="text-gray-400"> / {workerCount}</span>
          </span>
        </div>

        {overview && (
          <>
            <div className="flex items-center gap-2">
              <HardDrive className="w-3.5 h-3.5 text-gray-500" />
              <span className="text-gray-600 dark:text-gray-400">
                存储: {((overview.used_storage_bytes / (overview.total_storage_bytes || 1)) * 100).toFixed(1)}%
              </span>
            </div>
            <div className="flex items-center gap-2">
              <Activity className="w-3.5 h-3.5 text-gray-500" />
              <span className="text-gray-600 dark:text-gray-400">
                CPU: {(overview.avg_cpu_usage * 100).toFixed(1)}%
              </span>
            </div>
          </>
        )}
      </div>
    </div>
  );
});

MasterNode.displayName = 'MasterNode';
