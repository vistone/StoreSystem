// ============================================================
// 存储节点 - 工作流图中的存储/资源节点
// ============================================================

'use client';

import { memo } from 'react';
import { Handle, Position, NodeProps } from 'reactflow';
import { HardDrive, MemoryStick, Cpu, Wifi, Database, Zap } from 'lucide-react';
import { formatBytes, formatPercent, getDiskHealthColor } from '@/lib/utils';

interface StorageNodeData {
  workerId: string;
  storageUsed: number;
  storageCapacity: number;
  storageRatio: number;
  diskHealth: string;
  memoryUsed: number;
  memoryTotal: number;
  memoryRatio: number;
  cpuRatio: number;
  connections: number;
  // ---- 写入统计（v0.3.0 新增） ----
  flushedCount?: number;         // 已入库条数
  flushedBytes?: number;         // 已入库字节
  pendingCount?: number;         // 缓存中未入库条数
  pendingBytes?: number;         // 缓存中未入库字节
  writeRatePerSec?: number;      // 写入速率 ops/sec
  writeBytesPerSec?: number;     // 写入带宽 bytes/sec
}

export const StorageNode = memo(({ data }: NodeProps<StorageNodeData>) => {
  const {
    workerId,
    storageUsed,
    storageCapacity,
    storageRatio,
    diskHealth,
    memoryUsed,
    memoryTotal,
    memoryRatio,
    cpuRatio,
    connections,
    flushedCount = 0,
    flushedBytes = 0,
    pendingCount = 0,
    pendingBytes = 0,
    writeRatePerSec = 0,
    writeBytesPerSec = 0,
  } = data;

  const diskColor = getDiskHealthColor(diskHealth);

  // 已入库 / 总写入 比例（用于进度条）
  const totalPut = flushedCount + pendingCount;
  const flushRatio = totalPut > 0 ? flushedCount / totalPut : 0;

  // 写入速率颜色：>1000 ops/sec 红色（高负载），>100 黄色，否则绿色
  const rateColor =
    writeRatePerSec > 1000 ? '#ef4444' : writeRatePerSec > 100 ? '#eab308' : '#22c55e';

  return (
    <div className="px-3 py-2 shadow-lg rounded-xl border-2 border-blue-400 bg-gradient-to-br from-blue-50 to-sky-100 dark:from-blue-950 dark:to-sky-900 min-w-[200px]">
      <Handle type="target" position={Position.Top} className="!bg-blue-500" />

      <div className="flex items-center gap-1.5 mb-1.5">
        <HardDrive className="w-4 h-4 text-blue-600" />
        <span className="font-semibold text-xs text-blue-800 dark:text-blue-200">
          {workerId} 资源
        </span>
      </div>

      {/* 存储 */}
      <div className="mb-1">
        <div className="flex justify-between text-[10px] text-gray-500 mb-0.5">
          <span>存储</span>
          <span>{formatBytes(storageUsed)} / {formatBytes(storageCapacity)}</span>
        </div>
        <div className="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{
              width: `${Math.min(storageRatio * 100, 100)}%`,
              backgroundColor: diskColor,
            }}
          />
        </div>
      </div>

      {/* 内存 */}
      <div className="mb-1">
        <div className="flex justify-between text-[10px] text-gray-500 mb-0.5">
          <span>内存</span>
          <span>{formatPercent(memoryRatio)}</span>
        </div>
        <div className="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{
              width: `${Math.min(memoryRatio * 100, 100)}%`,
              backgroundColor:
                memoryRatio > 0.8 ? '#ef4444' : memoryRatio > 0.6 ? '#eab308' : '#3b82f6',
            }}
          />
        </div>
      </div>

      {/* CPU */}
      <div className="mb-1">
        <div className="flex justify-between text-[10px] text-gray-500 mb-0.5">
          <span>CPU</span>
          <span>{formatPercent(cpuRatio)}</span>
        </div>
        <div className="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-500"
            style={{
              width: `${Math.min(cpuRatio * 100, 100)}%`,
              backgroundColor:
                cpuRatio > 0.8 ? '#ef4444' : cpuRatio > 0.6 ? '#eab308' : '#3b82f6',
            }}
          />
        </div>
      </div>

      {/* ===== 写入统计区域（v0.3.0 新增） ===== */}
      <div className="mt-2 pt-2 border-t border-blue-200 dark:border-blue-800">
        <div className="flex items-center gap-1 mb-1">
          <Database className="w-3 h-3 text-emerald-600" />
          <span className="text-[10px] font-semibold text-emerald-700 dark:text-emerald-300">
            写入入库
          </span>
        </div>

        {/* 已入库 / 待入库 进度条 */}
        <div className="mb-1">
          <div className="flex justify-between text-[10px] text-gray-500 mb-0.5">
            <span>已入库 {flushedCount.toLocaleString()} 条</span>
            <span>{formatBytes(flushedBytes)}</span>
          </div>
          <div className="h-2 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden flex">
            <div
              className="h-full bg-emerald-500 transition-all duration-500"
              style={{ width: `${Math.min(flushRatio * 100, 100)}%` }}
            />
          </div>
          <div className="flex justify-between text-[10px] text-gray-500 mt-0.5">
            <span className="text-amber-600 dark:text-amber-400">
              缓存待入库 {pendingCount.toLocaleString()} 条
            </span>
            <span className="text-amber-600 dark:text-amber-400">
              {formatBytes(pendingBytes)}
            </span>
          </div>
        </div>

        {/* 写入速率 */}
        <div className="flex items-center gap-1 text-[10px] mt-1">
          <Zap className="w-3 h-3" style={{ color: rateColor }} />
          <span className="text-gray-500">写入速度</span>
          <span className="font-mono font-semibold" style={{ color: rateColor }}>
            {writeRatePerSec < 1
              ? writeRatePerSec.toFixed(2)
              : writeRatePerSec.toLocaleString(undefined, { maximumFractionDigits: 0 })}
            {' ops/s'}
          </span>
          <span className="ml-auto font-mono text-gray-500">
            {formatBytes(writeBytesPerSec)}/s
          </span>
        </div>
      </div>

      {/* 连接数 */}
      <div className="flex items-center gap-1 text-[10px] text-gray-500 mt-1.5 pt-1.5 border-t border-blue-200 dark:border-blue-800">
        <Wifi className="w-3 h-3" />
        <span>{connections} 连接</span>
        <span className="ml-auto">磁盘: {diskHealth}</span>
      </div>

      <Handle type="source" position={Position.Bottom} className="!bg-blue-500" />
    </div>
  );
});

StorageNode.displayName = 'StorageNode';
