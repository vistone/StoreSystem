// ============================================================
// WebSocket Hook - 实时数据推送
// ============================================================

'use client';

import { useEffect, useRef } from 'react';
import { createLogWebSocket } from '@/lib/api';
import { useClusterStore } from '@/stores/cluster-store';
import type { WsMessage } from '@/types';

export function useWebSocket() {
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimer = useRef<NodeJS.Timeout | null>(null);
  const {
    setWsConnected,
    addLogs,
    updateWorkersFromWs,
    fetchOverview,
    fetchLogStats,
  } = useClusterStore();

  useEffect(() => {
    function connect() {
      if (wsRef.current?.readyState === WebSocket.OPEN) return;

      try {
        const ws = createLogWebSocket();
        wsRef.current = ws;

        ws.onopen = () => {
          console.log('[WS] 已连接');
          setWsConnected(true);
          if (reconnectTimer.current) {
            clearTimeout(reconnectTimer.current);
            reconnectTimer.current = null;
          }
        };

        ws.onmessage = (event) => {
          try {
            const msg: WsMessage = JSON.parse(event.data);
            switch (msg.type) {
              case 'logs':
                addLogs(msg.data);
                break;
              case 'overview':
                updateWorkersFromWs(msg.data.workers);
                break;
            }
          } catch (e) {
            console.error('[WS] 消息解析失败:', e);
          }
        };

        ws.onclose = () => {
          console.log('[WS] 连接断开');
          setWsConnected(false);
          scheduleReconnect();
        };

        ws.onerror = (err) => {
          console.error('[WS] 错误:', err);
          ws.close();
        };
      } catch (e) {
        console.error('[WS] 连接失败:', e);
        scheduleReconnect();
      }
    }

    function scheduleReconnect() {
      if (reconnectTimer.current) return;
      reconnectTimer.current = setTimeout(() => {
        reconnectTimer.current = null;
        connect();
      }, 5000);
    }

    connect();

    return () => {
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
      if (reconnectTimer.current) {
        clearTimeout(reconnectTimer.current);
        reconnectTimer.current = null;
      }
    };
  }, [setWsConnected, addLogs, updateWorkersFromWs, fetchOverview, fetchLogStats]);
}
