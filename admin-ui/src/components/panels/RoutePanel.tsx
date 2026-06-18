// ============================================================
// 路由规则面板 - 展示 key 前缀路由规则
// ============================================================

'use client';

import { useEffect } from 'react';
import { GitBranch, ArrowRight, RefreshCw } from 'lucide-react';
import { useClusterStore } from '@/stores/cluster-store';
import { formatTimestamp } from '@/lib/utils';

export function RoutePanel() {
  const { routes, routesLoading, fetchRoutes, autoRefresh } = useClusterStore();

  useEffect(() => {
    fetchRoutes();
    if (!autoRefresh) return;
    const interval = setInterval(fetchRoutes, 10000);
    return () => clearInterval(interval);
  }, [fetchRoutes, autoRefresh]);

  return (
    <div className="flex flex-col h-full bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700">
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-100 dark:border-gray-700">
        <div className="flex items-center gap-2">
          <GitBranch className="w-4 h-4 text-purple-500" />
          <span className="font-semibold text-sm text-gray-700 dark:text-gray-300">
            路由规则
          </span>
          <span className="text-xs text-gray-400">({routes.length} 条)</span>
        </div>
        <button
          onClick={() => fetchRoutes()}
          className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${routesLoading ? 'animate-spin' : ''}`} />
        </button>
      </div>

      <div className="flex-1 overflow-auto p-3">
        {routes.length === 0 ? (
          <div className="flex items-center justify-center h-full text-gray-400 text-sm">
            暂无路由规则
          </div>
        ) : (
          <div className="space-y-2">
            {routes.map((rule) => (
              <div
                key={rule.key_prefix}
                className="flex items-center gap-3 p-2.5 bg-gray-50 dark:bg-gray-900/50 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-900 transition-colors"
              >
                {/* 前缀 */}
                <div className="flex-1">
                  <div className="flex items-center gap-2">
                    <span className="px-2 py-0.5 bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 rounded text-xs font-mono font-medium">
                      {rule.key_prefix}
                    </span>
                    <ArrowRight className="w-3 h-3 text-gray-400" />
                    <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
                      {rule.worker_id}
                    </span>
                  </div>
                </div>

                {/* 优先级 */}
                <div className="flex items-center gap-2">
                  <span className="text-[10px] text-gray-400">优先级:</span>
                  <span className="text-xs font-mono text-gray-600 dark:text-gray-400">
                    {rule.priority}
                  </span>
                </div>

                {/* 创建时间 */}
                <div className="text-[10px] text-gray-400">
                  {formatTimestamp(rule.created_at)}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
