// ============================================================
// 侧边栏导航
// ============================================================

'use client';

import {
  Network,
  Table2,
  FileText,
  GitBranch,
  Settings,
  Wifi,
  WifiOff,
} from 'lucide-react';
import { useClusterStore } from '@/stores/cluster-store';

export type TabId = 'workflow' | 'workers' | 'logs' | 'routes' | 'settings';

interface SidebarProps {
  activeTab: TabId;
  onTabChange: (tab: TabId) => void;
}

const tabs: { id: TabId; label: string; icon: any }[] = [
  { id: 'workflow', label: '工作流', icon: Network },
  { id: 'workers', label: 'Worker 列表', icon: Table2 },
  { id: 'logs', label: '日志', icon: FileText },
  { id: 'routes', label: '路由规则', icon: GitBranch },
  { id: 'settings', label: '设置', icon: Settings },
];

export function Sidebar({ activeTab, onTabChange }: SidebarProps) {
  const { wsConnected } = useClusterStore();

  return (
    <div className="w-16 lg:w-56 bg-white dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 flex flex-col">
      {/* Logo */}
      <div className="h-14 flex items-center justify-center lg:justify-start lg:px-4 border-b border-gray-200 dark:border-gray-700">
        <div className="w-8 h-8 bg-gradient-to-br from-indigo-500 to-purple-600 rounded-lg flex items-center justify-center">
          <Network className="w-4 h-4 text-white" />
        </div>
        <span className="hidden lg:block ml-2 font-bold text-gray-800 dark:text-gray-200">
          Store Admin
        </span>
      </div>

      {/* 导航 */}
      <nav className="flex-1 py-3">
        {tabs.map((tab) => {
          const isActive = activeTab === tab.id;
          return (
            <button
              key={tab.id}
              onClick={() => onTabChange(tab.id)}
              className={`w-full flex items-center justify-center lg:justify-start gap-3 px-3 lg:px-4 py-2.5 transition-colors ${
                isActive
                  ? 'bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400 border-r-2 border-indigo-500'
                  : 'text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-700/50'
              }`}
            >
              <tab.icon className="w-5 h-5 flex-shrink-0" />
              <span className="hidden lg:block text-sm font-medium">{tab.label}</span>
            </button>
          );
        })}
      </nav>

      {/* 连接状态 */}
      <div className="p-3 border-t border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-center lg:justify-start gap-2">
          {wsConnected ? (
            <Wifi className="w-3.5 h-3.5 text-green-500" />
          ) : (
            <WifiOff className="w-3.5 h-3.5 text-red-500" />
          )}
          <span className="hidden lg:block text-[10px] text-gray-400">
            {wsConnected ? '实时连接' : '已断开'}
          </span>
        </div>
      </div>
    </div>
  );
}
