// ============================================================
// 主仪表盘 - 管理界面核心布局
// ============================================================

'use client';

import { useState } from 'react';
import { Sidebar, TabId } from '@/components/ui/Sidebar';
import { OverviewPanel } from '@/components/panels/OverviewPanel';
import { ClusterWorkflow } from '@/components/workflow/ClusterWorkflow';
import { WorkerDetailPanel } from '@/components/panels/WorkerDetailPanel';
import { LogPanel } from '@/components/panels/LogPanel';
import { RoutePanel } from '@/components/panels/RoutePanel';
import { ConfigPanel } from '@/components/panels/ConfigPanel';
import { PendingPanel } from '@/components/panels/PendingPanel';
import { useWebSocket } from '@/hooks/useWebSocket';
import { useClusterStore } from '@/stores/cluster-store';
import { Wifi, WifiOff } from 'lucide-react';

export function Dashboard() {
  const [activeTab, setActiveTab] = useState<TabId>('workflow');
  const { autoRefresh, setAutoRefresh, wsConnected } = useClusterStore();

  // 启动 WebSocket 实时连接
  useWebSocket();

  return (
    <div className="flex h-screen bg-gray-50 dark:bg-gray-900">
      {/* 侧边栏 */}
      <Sidebar activeTab={activeTab} onTabChange={setActiveTab} />

      {/* 主内容区 */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* 顶部栏 */}
        <header className="h-14 bg-white dark:bg-gray-800 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between px-4 lg:px-6">
          <div className="flex items-center gap-3">
            <h1 className="text-lg font-bold text-gray-800 dark:text-gray-200">
              {activeTab === 'workflow' && '集群工作流'}
              {activeTab === 'workers' && 'Worker 节点列表'}
              {activeTab === 'logs' && '实时日志'}
              {activeTab === 'routes' && '路由规则'}
              {activeTab === 'pending' && '待处理缓存'}
              {activeTab === 'settings' && '设置'}
            </h1>
          </div>

          <div className="flex items-center gap-3">
            {/* 自动刷新开关 */}
            <label className="flex items-center gap-2 text-xs text-gray-500">
              <span>自动刷新</span>
              <button
                onClick={() => setAutoRefresh(!autoRefresh)}
                className={`relative w-8 h-4 rounded-full transition-colors ${
                  autoRefresh ? 'bg-indigo-500' : 'bg-gray-300 dark:bg-gray-600'
                }`}
              >
                <div
                  className={`absolute top-0.5 w-3 h-3 bg-white rounded-full shadow transition-transform ${
                    autoRefresh ? 'translate-x-4' : 'translate-x-0.5'
                  }`}
                />
              </button>
            </label>

            {/* 连接状态 */}
            <div className="flex items-center gap-1.5 text-xs">
              {wsConnected ? (
                <>
                  <Wifi className="w-3 h-3 text-green-500" />
                  <span className="text-green-600 dark:text-green-400">已连接</span>
                </>
              ) : (
                <>
                  <WifiOff className="w-3 h-3 text-red-500" />
                  <span className="text-red-600 dark:text-red-400">已断开</span>
                </>
              )}
            </div>
          </div>
        </header>

        {/* 内容区 */}
        <main className="flex-1 overflow-hidden">
          {activeTab === 'workflow' && (
            <div className="h-full flex flex-col">
              {/* 概览卡片 */}
              <div className="flex-shrink-0 p-3 lg:p-4">
                <OverviewPanel />
              </div>
              {/* 工作流图 */}
              <div className="flex-1 p-3 lg:p-4 pt-0">
                <div className="h-full bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700 overflow-hidden">
                  <ClusterWorkflow />
                </div>
              </div>
            </div>
          )}

          {activeTab === 'workers' && (
            <div className="h-full p-3 lg:p-4">
              <WorkerDetailPanel />
            </div>
          )}

          {activeTab === 'logs' && (
            <div className="h-full p-3 lg:p-4">
              <LogPanel />
            </div>
          )}

          {activeTab === 'routes' && (
            <div className="h-full p-3 lg:p-4">
              <RoutePanel />
            </div>
          )}

          {activeTab === 'pending' && (
            <div className="h-full p-3 lg:p-4">
              <PendingPanel />
            </div>
          )}

          {activeTab === 'settings' && (
            <div className="h-full p-3 lg:p-4">
              <ConfigPanel />
            </div>
          )}
        </main>
      </div>
    </div>
  );
}
