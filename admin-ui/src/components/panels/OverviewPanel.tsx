// ============================================================
// 集群概览面板 - 顶部统计卡片
// ============================================================

'use client';

import { useEffect } from 'react';
import {
  Server,
  HardDrive,
  Activity,
  AlertTriangle,
  FileText,
  Wifi,
  Users,
} from 'lucide-react';
import { useClusterStore } from '@/stores/cluster-store';
import { formatBytes, formatPercent } from '@/lib/utils';

export function OverviewPanel() {
  const { overview, fetchOverview, autoRefresh } = useClusterStore();

  useEffect(() => {
    fetchOverview();
    if (!autoRefresh) return;
    const interval = setInterval(fetchOverview, 5000);
    return () => clearInterval(interval);
  }, [fetchOverview, autoRefresh]);

  if (!overview) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3 animate-pulse">
        {Array.from({ length: 7 }).map((_, i) => (
          <div key={i} className="h-24 bg-gray-200 dark:bg-gray-700 rounded-xl" />
        ))}
      </div>
    );
  }

  const cards = [
    {
      title: 'Worker 节点',
      value: `${overview.alive_workers} / ${overview.total_workers}`,
      sub: `宕机: ${overview.dead_workers}`,
      icon: Server,
      color: 'text-green-600 bg-green-100 dark:bg-green-900/30',
    },
    {
      title: '存储使用',
      value: formatBytes(overview.used_storage_bytes),
      sub: `总量: ${formatBytes(overview.total_storage_bytes)}`,
      icon: HardDrive,
      color: 'text-blue-600 bg-blue-100 dark:bg-blue-900/30',
    },
    {
      title: '内存使用',
      value: formatBytes(overview.used_memory_bytes),
      sub: `总量: ${formatBytes(overview.total_memory_bytes)}`,
      icon: Activity,
      color: 'text-purple-600 bg-purple-100 dark:bg-purple-900/30',
    },
    {
      title: '平均 CPU',
      value: formatPercent(overview.avg_cpu_usage),
      sub: '集群负载',
      icon: Wifi,
      color: 'text-cyan-600 bg-cyan-100 dark:bg-cyan-900/30',
    },
    {
      title: '今日日志',
      value: overview.total_logs_today.toLocaleString(),
      sub: `未读: ${overview.unread_logs}`,
      icon: FileText,
      color: 'text-amber-600 bg-amber-100 dark:bg-amber-900/30',
    },
    {
      title: '今日错误',
      value: overview.error_logs_today.toLocaleString(),
      sub: '需关注',
      icon: AlertTriangle,
      color: overview.error_logs_today > 0
        ? 'text-red-600 bg-red-100 dark:bg-red-900/30'
        : 'text-gray-600 bg-gray-100 dark:bg-gray-900/30',
    },
    {
      title: '集群状态',
      value: overview.dead_workers > 0 ? '⚠️ 异常' : '✅ 健康',
      sub: `Worker: ${overview.alive_workers}`,
      icon: Users,
      color: overview.dead_workers > 0
        ? 'text-orange-600 bg-orange-100 dark:bg-orange-900/30'
        : 'text-green-600 bg-green-100 dark:bg-green-900/30',
    },
  ];

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3">
      {cards.map((card) => (
        <div
          key={card.title}
          className="bg-white dark:bg-gray-800 rounded-xl p-3 shadow-sm border border-gray-100 dark:border-gray-700 hover:shadow-md transition-shadow"
        >
          <div className="flex items-center gap-2 mb-2">
            <div className={`p-1.5 rounded-lg ${card.color}`}>
              <card.icon className="w-4 h-4" />
            </div>
            <span className="text-xs text-gray-500 dark:text-gray-400">{card.title}</span>
          </div>
          <div className="text-lg font-bold text-gray-800 dark:text-gray-200">{card.value}</div>
          <div className="text-[10px] text-gray-400 mt-0.5">{card.sub}</div>
        </div>
      ))}
    </div>
  );
}
