// ============================================================
// Pending 缓存面板 - 展示各区域待处理缓存状态
// ============================================================

'use client';

import { useEffect } from 'react';
import { HardDrive, RefreshCw, Trash2 } from 'lucide-react';
import { useClusterStore } from '@/stores/cluster-store';
import { formatBytes } from '@/lib/utils';

const REGIONS = ['0', '1', '2', '3'];

export function PendingPanel() {
  const { pendingStats, pendingLoading, fetchPending, clearPending, autoRefresh } =
    useClusterStore();

  useEffect(() => {
    fetchPending();
    if (!autoRefresh) return;
    const interval = setInterval(fetchPending, 5000);
    return () => clearInterval(interval);
  }, [fetchPending, autoRefresh]);

  const handleClear = async (region: string) => {
    await clearPending(region);
  };

  const maxCount = pendingStats
    ? Math.max(1, ...REGIONS.map((r) => pendingStats.regions[r]?.count || 0))
    : 1;

  return (
    <div className="flex flex-col h-full bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700">
      {/* 头部 */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-100 dark:border-gray-700">
        <div className="flex items-center gap-2">
          <HardDrive className="w-4 h-4 text-indigo-500" />
          <span className="font-semibold text-sm text-gray-700 dark:text-gray-300">
            待处理缓存
          </span>
        </div>
        <button
          onClick={() => fetchPending()}
          className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${pendingLoading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      {/* 内容 */}
      <div className="flex-1 overflow-auto p-3">
        {/* 加载骨架 */}
        {pendingLoading && !pendingStats && (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 animate-pulse">
            {REGIONS.map((region) => (
              <div
                key={region}
                className="h-36 bg-gray-200 dark:bg-gray-700 rounded-xl"
              />
            ))}
          </div>
        )}

        {/* 空状态 */}
        {!pendingLoading && !pendingStats && (
          <div className="flex items-center justify-center h-full text-gray-400 text-sm">
            暂无待处理数据
          </div>
        )}

        {/* 区域卡片 */}
        {pendingStats && (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            {REGIONS.map((region) => {
              const stat = pendingStats.regions[region];
              const count = stat?.count || 0;
              const bytes = stat?.bytes || 0;
              const ratio = maxCount > 0 ? count / maxCount : 0;

              return (
                <div
                  key={region}
                  className="bg-gray-50 dark:bg-gray-900/50 rounded-xl p-3 border border-gray-100 dark:border-gray-700 hover:shadow-md transition-shadow"
                >
                  {/* 区域标签 */}
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase">
                      Region {region}
                    </span>
                    <button
                      onClick={() => handleClear(region)}
                      disabled={count === 0}
                      className="p-1 rounded-md text-gray-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                      title="清除该区域缓存"
                    >
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>

                  {/* 条目数 */}
                  <div className="text-2xl font-bold text-gray-800 dark:text-gray-200 mb-1">
                    {count.toLocaleString()}
                  </div>

                  {/* 字节数 */}
                  <div className="text-xs text-gray-500 dark:text-gray-400 mb-2">
                    {formatBytes(bytes)}
                  </div>

                  {/* 进度条 */}
                  <div className="w-full h-1.5 bg-gray-200 dark:bg-gray-700 rounded-full overflow-hidden">
                    <div
                      className="h-full bg-gradient-to-r from-indigo-400 to-indigo-600 rounded-full transition-all duration-300"
                      style={{ width: `${ratio * 100}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
