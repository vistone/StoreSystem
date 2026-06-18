// ============================================================
// 集群工作流图 - 使用 React Flow 展示集群拓扑
// ============================================================

'use client';

import { useEffect, useRef } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  Node,
  Edge,
  useNodesState,
  useEdgesState,
  useReactFlow,
  ReactFlowProvider,
  MarkerType,
  BackgroundVariant,
} from 'reactflow';
import 'reactflow/dist/style.css';
import { useClusterStore } from '@/stores/cluster-store';
import { MasterNode } from './MasterNode';
import { WorkerNode } from './WorkerNode';
import { StorageNode } from './StorageNode';

const nodeTypes = {
  master: MasterNode,
  worker: WorkerNode,
  storage: StorageNode,
};

// 节点尺寸估算（用于碰撞检测）
const NODE_WIDTH = 220;
const NODE_HEIGHT = 160;
const MIN_SPACING = 40; // 节点之间最小间距

// 位置持久化存储 key
const POSITIONS_STORAGE_KEY = 'cluster_workflow_positions_v1';
// 防抖写入 localStorage 的延迟（ms）
const PERSIST_DEBOUNCE_MS = 400;

type Position = { x: number; y: number };

// 安全读写 localStorage（SSR 环境下 window 不存在）
function loadPositions(): Map<string, Position> {
  if (typeof window === 'undefined') return new Map();
  try {
    const raw = window.localStorage.getItem(POSITIONS_STORAGE_KEY);
    if (!raw) return new Map();
    const obj = JSON.parse(raw) as Record<string, Position>;
    return new Map(Object.entries(obj));
  } catch {
    return new Map();
  }
}

function savePositions(positions: Map<string, Position>) {
  if (typeof window === 'undefined') return;
  try {
    const obj: Record<string, Position> = {};
    positions.forEach((v, k) => {
      obj[k] = v;
    });
    window.localStorage.setItem(POSITIONS_STORAGE_KEY, JSON.stringify(obj));
  } catch {
    // 忽略写入失败（如 quota 超限）
  }
}

export function ClusterWorkflow() {
  // ReactFlowProvider 确保 useReactFlow 钩子可用
  return (
    <ReactFlowProvider>
      <ClusterWorkflowInner />
    </ReactFlowProvider>
  );
}

function ClusterWorkflowInner() {
  const { workers, overview, fetchWorkers, autoRefresh } = useClusterStore();
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const { fitView } = useReactFlow();
  // 记录是否是首次加载：首次加载自动 fitView，之后只在节点数量变化时才 fitView
  // 避免用户手动缩放/平移后被频繁重置
  const isFirstFitRef = useRef(true);
  const prevNodeCountRef = useRef(0);
  // 标记待执行的 fitView（由节点构建 effect 设置，由 nodes 渲染 effect 执行）
  const pendingFitRef = useRef(false);

  // 使用 ref 持久化节点位置：
  // - 初始值从 localStorage 读取（页面刷新后恢复用户编排的位置）
  // - 运行时通过 effect 监听 nodes 变化（用户拖动）同步到 ref
  // - 防抖写入 localStorage，避免频繁 IO
  const positionsRef = useRef<Map<string, Position>>(loadPositions());
  const saveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // 同步 nodes 的位置到 ref + 防抖写入 localStorage（监听用户拖动）
  useEffect(() => {
    let changed = false;
    nodes.forEach((n) => {
      const prev = positionsRef.current.get(n.id);
      if (!prev || prev.x !== n.position.x || prev.y !== n.position.y) {
        positionsRef.current.set(n.id, { ...n.position });
        changed = true;
      }
    });

    // 只有位置真的变化了才写入 localStorage
    if (changed) {
      if (saveTimerRef.current) clearTimeout(saveTimerRef.current);
      saveTimerRef.current = setTimeout(() => {
        savePositions(positionsRef.current);
        saveTimerRef.current = null;
      }, PERSIST_DEBOUNCE_MS);
    }
  }, [nodes]);

  // 组件卸载时确保最后一次位置变更被写入
  useEffect(() => {
    return () => {
      if (saveTimerRef.current) {
        clearTimeout(saveTimerRef.current);
        savePositions(positionsRef.current);
      }
    };
  }, []);

  // 工作流页面独立定时刷新 worker 列表，确保新上线的 worker 能及时显示
  useEffect(() => {
    fetchWorkers();
    if (!autoRefresh) return;
    const interval = setInterval(fetchWorkers, 5000);
    return () => clearInterval(interval);
  }, [fetchWorkers, autoRefresh]);

  // 计算一个不与现有节点重叠的位置
  const findNonOverlappingPosition = (
    preferred: Position,
    existing: Position[]
  ): Position => {
    const isOverlap = (p: Position) =>
      existing.some(
        (e) =>
          Math.abs(e.x - p.x) < NODE_WIDTH + MIN_SPACING &&
          Math.abs(e.y - p.y) < NODE_HEIGHT + MIN_SPACING
      );

    if (!isOverlap(preferred)) return preferred;

    // 螺旋搜索：在 preferred 周围找不重叠的位置
    const step = NODE_WIDTH + MIN_SPACING;
    for (let radius = 1; radius <= 10; radius++) {
      for (let i = 0; i < radius * 8; i++) {
        const angle = (2 * Math.PI * i) / (radius * 8);
        const candidate = {
          x: preferred.x + radius * step * Math.cos(angle),
          y: preferred.y + radius * step * Math.sin(angle),
        };
        if (!isOverlap(candidate)) return candidate;
      }
    }
    return preferred;
  };

  // 构建工作流节点
  useEffect(() => {
    const flowNodes: Node[] = [];
    const flowEdges: Edge[] = [];
    const usedPositions: Position[] = [];

    // 获取已保存的位置（从 ref 读取，不依赖 nodes state）
    const getSavedPosition = (id: string): Position | undefined => {
      return positionsRef.current.get(id);
    };

    const claimPosition = (id: string, fallback: Position): Position => {
      const saved = getSavedPosition(id);
      let pos: Position;
      if (saved) {
        pos = { ...saved };
      } else {
        // 新节点：在 fallback 基础上找不重叠的位置
        pos = findNonOverlappingPosition(fallback, usedPositions);
      }
      usedPositions.push(pos);
      return pos;
    };

    // Master 节点（居中）
    const masterPos = claimPosition('master', { x: 400, y: 50 });
    flowNodes.push({
      id: 'master',
      type: 'master',
      position: masterPos,
      data: {
        label: 'Master',
        overview,
        workerCount: workers.length,
        aliveCount: workers.filter((w) => w.alive).length,
      },
    });

    // Worker 节点（环绕 Master 排列）
    const centerX = 400;
    const centerY = 250;
    const radiusX = 320;
    const radiusY = 220;

    workers.forEach((worker, index) => {
      const angle = (2 * Math.PI * index) / Math.max(workers.length, 1) - Math.PI / 2;
      const x = centerX + radiusX * Math.cos(angle);
      const y = centerY + radiusY * Math.sin(angle);

      const workerId = `worker-${worker.worker_id}`;
      const storageId = `storage-${worker.worker_id}`;

      const workerPos = claimPosition(workerId, { x, y });
      flowNodes.push({
        id: workerId,
        type: 'worker',
        position: workerPos,
        data: { worker },
      });

      // Master -> Worker 连线
      flowEdges.push({
        id: `edge-master-${worker.worker_id}`,
        source: 'master',
        target: workerId,
        animated: worker.alive,
        style: {
          stroke: worker.alive ? '#22c55e' : '#6b7280',
          strokeWidth: 2,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: worker.alive ? '#22c55e' : '#6b7280',
        },
        label: worker.alive ? '🟢' : '🔴',
      });

      // Worker -> Storage 节点（相对 worker 偏移）
      const storagePos = claimPosition(storageId, {
        x: workerPos.x + 140,
        y: workerPos.y + 100,
      });
      flowNodes.push({
        id: storageId,
        type: 'storage',
        position: storagePos,
        data: {
          workerId: worker.worker_id,
          storageUsed: worker.storage_used_bytes,
          storageCapacity: worker.storage_capacity_bytes,
          storageRatio: worker.storage_usage_ratio,
          diskHealth: worker.disk_health,
          memoryUsed: worker.memory_used_bytes,
          memoryTotal: worker.memory_total_bytes,
          memoryRatio: worker.memory_usage_ratio,
          cpuRatio: worker.cpu_usage_ratio,
          connections: worker.active_connections,
          // ---- 写入统计（v0.3.0 新增） ----
          flushedCount: worker.flushed_count,
          flushedBytes: worker.flushed_bytes,
          pendingCount: worker.pending_count,
          pendingBytes: worker.pending_bytes,
          writeRatePerSec: worker.write_rate_per_sec,
          writeBytesPerSec: worker.write_bytes_per_sec,
        },
      });

      flowEdges.push({
        id: `edge-worker-${worker.worker_id}-storage`,
        source: workerId,
        target: storageId,
        style: { stroke: '#3b82f6', strokeWidth: 1.5, strokeDasharray: '5 5' },
        markerEnd: { type: MarkerType.ArrowClosed, color: '#3b82f6' },
      });
    });

    // 清理 ref 中已不存在的节点位置（避免内存泄漏）
    const currentIds = new Set(flowNodes.map((n) => n.id));
    Array.from(positionsRef.current.keys()).forEach((id) => {
      if (!currentIds.has(id)) {
        positionsRef.current.delete(id);
      }
    });

    setNodes(flowNodes);
    setEdges(flowEdges);

    // 标记需要 fitView（实际 fitView 在下面的 effect 中执行，确保 nodes 已渲染）
    const nodeCount = flowNodes.length;
    const shouldFit =
      isFirstFitRef.current || nodeCount !== prevNodeCountRef.current;
    prevNodeCountRef.current = nodeCount;
    pendingFitRef.current = shouldFit;
  }, [workers, overview, setNodes, setEdges]);

  // 监听 nodes 变化，在节点渲染完成后执行 fitView
  // 这样能确保 ReactFlow 内部已测量到节点尺寸，fitView 才能正确计算全局视图
  useEffect(() => {
    if (!pendingFitRef.current || nodes.length === 0) return;
    pendingFitRef.current = false;
    // 用 setTimeout(0) 等待 ReactFlow 完成 DOM 渲染和尺寸测量
    const timer = setTimeout(() => {
      fitView({ padding: 0.2, duration: 300 });
      isFirstFitRef.current = false;
    }, 50);
    return () => clearTimeout(timer);
  }, [nodes, fitView]);

  return (
    <div className="w-full h-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        minZoom={0.05}
        maxZoom={2}
        attributionPosition="bottom-left"
      >
        <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
        <Controls />
        <MiniMap
          nodeStrokeColor="#6b7280"
          nodeColor={(node) => {
            if (node.type === 'master') return '#6366f1';
            if (node.type === 'worker') {
              const alive = node.data?.worker?.alive;
              return alive ? '#22c55e' : '#6b7280';
            }
            return '#3b82f6';
          }}
          maskColor="rgba(0,0,0,0.1)"
        />
      </ReactFlow>
    </div>
  );
}
