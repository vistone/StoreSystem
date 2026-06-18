// ============================================================
// 日志面板 - 实时日志查看器
// ============================================================

'use client';

import { useEffect, useState, useRef } from 'react';
import {
  FileText,
  Filter,
  CheckCheck,
  AlertTriangle,
  Info,
  AlertCircle,
  Bug,
  X,
  RefreshCw,
} from 'lucide-react';
import { useClusterStore } from '@/stores/cluster-store';
import { formatTimestamp, getLevelColor } from '@/lib/utils';
import type { LogEntry } from '@/types';

const LEVELS = ['all', 'debug', 'info', 'warning', 'error', 'critical'];
const LEVEL_ICONS: Record<string, any> = {
  debug: Bug,
  info: Info,
  warning: AlertTriangle,
  error: AlertCircle,
  critical: AlertCircle,
};

export function LogPanel() {
  const {
    logs,
    logTotal,
    logsLoading,
    fetchLogs,
    acknowledgeLog,
    acknowledgeAllLogs,
    autoRefresh,
  } = useClusterStore();

  const [filterLevel, setFilterLevel] = useState('all');
  const [filterWorker, setFilterWorker] = useState('');
  const [showFilters, setShowFilters] = useState(false);
  const scrollRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  useEffect(() => {
    fetchLogs({ limit: 200 });
    if (!autoRefresh) return;
    const interval = setInterval(() => fetchLogs({ limit: 200 }), 3000);
    return () => clearInterval(interval);
  }, [fetchLogs, autoRefresh]);

  // 自动滚动
  useEffect(() => {
    if (autoScroll && scrollRef.current) {
      scrollRef.current.scrollTop = 0;
    }
  }, [logs, autoScroll]);

  const filteredLogs = logs.filter((log) => {
    if (filterLevel !== 'all' && log.level !== filterLevel) return false;
    if (filterWorker && log.worker_id !== filterWorker) return false;
    return true;
  });

  const uniqueWorkers = [...new Set(logs.map((l) => l.worker_id))];

  return (
    <div className="flex flex-col h-full bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700">
      {/* 标题栏 */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-100 dark:border-gray-700">
        <div className="flex items-center gap-2">
          <FileText className="w-4 h-4 text-amber-500" />
          <span className="font-semibold text-sm text-gray-700 dark:text-gray-300">
            实时日志
          </span>
          <span className="text-xs text-gray-400">({logTotal} 条)</span>
        </div>
        <div className="flex items-center gap-1.5">
          <button
            onClick={() => setShowFilters(!showFilters)}
            className={`p-1.5 rounded-lg transition-colors ${
              showFilters
                ? 'bg-amber-100 text-amber-600 dark:bg-amber-900/30'
                : 'text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700'
            }`}
          >
            <Filter className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => acknowledgeAllLogs()}
            className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
            title="全部标记已读"
          >
            <CheckCheck className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={() => fetchLogs({ limit: 200 })}
            className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
            title="刷新"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${logsLoading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      {/* 过滤器 */}
      {showFilters && (
        <div className="px-4 py-2 border-b border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/50">
          <div className="flex items-center gap-3 flex-wrap">
            <div className="flex items-center gap-1">
              <span className="text-xs text-gray-500">级别:</span>
              <div className="flex gap-0.5">
                {LEVELS.map((level) => (
                  <button
                    key={level}
                    onClick={() => setFilterLevel(level)}
                    className={`px-2 py-0.5 text-[10px] rounded-md transition-colors ${
                      filterLevel === level
                        ? 'bg-amber-500 text-white'
                        : 'text-gray-500 hover:bg-gray-200 dark:hover:bg-gray-700'
                    }`}
                  >
                    {level === 'all' ? '全部' : level}
                  </button>
                ))}
              </div>
            </div>
            <div className="flex items-center gap-1">
              <span className="text-xs text-gray-500">Worker:</span>
              <select
                value={filterWorker}
                onChange={(e) => setFilterWorker(e.target.value)}
                className="text-xs border border-gray-200 dark:border-gray-600 rounded-md px-2 py-0.5 bg-white dark:bg-gray-800"
              >
                <option value="">全部</option>
                {uniqueWorkers.map((w) => (
                  <option key={w} value={w}>{w}</option>
                ))}
              </select>
            </div>
          </div>
        </div>
      )}

      {/* 日志列表 */}
      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto"
        onScroll={(e) => {
          const el = e.currentTarget;
          setAutoScroll(el.scrollTop < 50);
        }}
      >
        {filteredLogs.length === 0 ? (
          <div className="flex items-center justify-center h-full text-gray-400 text-sm">
            暂无日志
          </div>
        ) : (
          <div className="divide-y divide-gray-50 dark:divide-gray-800">
            {filteredLogs.map((log) => (
              <LogRow key={log.id} log={log} onAck={acknowledgeLog} />
            ))}
          </div>
        )}
      </div>

      {/* 底部状态 */}
      <div className="px-4 py-1.5 border-t border-gray-100 dark:border-gray-700 flex items-center justify-between text-[10px] text-gray-400">
        <span>
          显示 {filteredLogs.length} / {logTotal} 条
        </span>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setAutoScroll(!autoScroll)}
            className={`px-2 py-0.5 rounded ${
              autoScroll ? 'bg-blue-100 text-blue-600' : 'text-gray-400'
            }`}
          >
            自动滚动
          </button>
          <span>
            {logs.filter((l) => !l.acknowledged).length} 条未读
          </span>
        </div>
      </div>
    </div>
  );
}

// 单条日志行
function LogRow({
  log,
  onAck,
}: {
  log: LogEntry;
  onAck: (id: number) => void;
}) {
  const LevelIcon = LEVEL_ICONS[log.level] || Info;
  const color = getLevelColor(log.level);

  return (
    <div
      className={`flex items-start gap-2 px-4 py-1.5 hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors text-xs ${
        !log.acknowledged ? 'bg-blue-50/30 dark:bg-blue-900/10' : ''
      }`}
    >
      {/* 级别图标 */}
      <div className="mt-0.5 flex-shrink-0">
        <LevelIcon className="w-3.5 h-3.5" style={{ color }} />
      </div>

      {/* 时间 */}
      <span className="text-gray-400 flex-shrink-0 w-32 font-mono text-[10px]">
        {formatTimestamp(log.timestamp)}
      </span>

      {/* Worker */}
      <span className="text-gray-500 flex-shrink-0 w-20 truncate">
        {log.worker_id}
      </span>

      {/* 级别标签 */}
      <span
        className="flex-shrink-0 px-1.5 py-0.5 rounded text-[10px] font-medium"
        style={{
          backgroundColor: `${color}20`,
          color,
        }}
      >
        {log.level}
      </span>

      {/* 分类 */}
      <span className="text-gray-400 flex-shrink-0 w-16">{log.category}</span>

      {/* 消息 */}
      <span className="flex-1 text-gray-700 dark:text-gray-300 truncate">
        {log.message}
      </span>

      {/* 已读按钮 */}
      {!log.acknowledged && (
        <button
          onClick={() => onAck(log.id)}
          className="flex-shrink-0 p-0.5 text-gray-300 hover:text-green-500 transition-colors"
          title="标记已读"
        >
          <CheckCheck className="w-3 h-3" />
        </button>
      )}
    </div>
  );
}
