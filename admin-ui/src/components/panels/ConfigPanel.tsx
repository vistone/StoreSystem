// ============================================================
// 配置管理面板 - 系统运行时配置编辑
// ============================================================

"use client";

import { useEffect, useState, useCallback, useRef } from "react";
import {
  Settings,
  Save,
  RotateCcw,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  RefreshCw,
  Info,
} from "lucide-react";
import { useClusterStore } from "@/stores/cluster-store";
import type { AllConfigs } from "@/types";

// ============================================================
// 配置项元数据
// ============================================================

type ReloadKind = "hot" | "restart" | "yaml";

interface FieldMeta {
  key: string;
  label: string;
  kind: "number" | "text" | "select";
  reload: ReloadKind;
  options?: string[];
  unit?: string;
}

interface ConfigSection {
  id: string;
  label: string;
  configKey: keyof AllConfigs;
  fields: FieldMeta[];
}

const SECTIONS: ConfigSection[] = [
  {
    id: "master",
    label: "Master",
    configKey: "master",
    fields: [
      {
        key: "heartbeat_timeout_secs",
        label: "心跳超时",
        kind: "number",
        reload: "hot",
        unit: "秒",
      },
      {
        key: "cleanup_interval_secs",
        label: "清理间隔",
        kind: "number",
        reload: "hot",
        unit: "秒",
      },
      {
        key: "max_message_size",
        label: "最大消息大小",
        kind: "number",
        reload: "hot",
        unit: "MB",
      },
      {
        key: "protocol",
        label: "通信协议",
        kind: "select",
        reload: "restart",
        options: ["grpc", "restful", "ws", "both"],
      },
    ],
  },
  {
    id: "worker",
    label: "Worker",
    configKey: "worker",
    fields: [
      { key: "cache_size", label: "缓存大小", kind: "number", reload: "hot" },
      {
        key: "flush_interval_ms",
        label: "刷盘间隔",
        kind: "number",
        reload: "hot",
        unit: "毫秒",
      },
      {
        key: "heartbeat_interval_secs",
        label: "心跳间隔",
        kind: "number",
        reload: "hot",
        unit: "秒",
      },
      { key: "kv_ext", label: "KV 扩展名", kind: "text", reload: "restart" },
      {
        key: "meta_ext",
        label: "Meta 扩展名",
        kind: "text",
        reload: "restart",
      },
    ],
  },
  {
    id: "pending",
    label: "Pending",
    configKey: "pending",
    fields: [
      {
        key: "gc_interval_secs",
        label: "GC 间隔",
        kind: "number",
        reload: "hot",
        unit: "秒",
      },
      {
        key: "flush_timeout_secs",
        label: "刷盘超时",
        kind: "number",
        reload: "hot",
        unit: "秒",
      },
    ],
  },
  {
    id: "guardian",
    label: "Guardian",
    configKey: "guardian",
    fields: [
      {
        key: "probe_interval_secs",
        label: "探测间隔",
        kind: "number",
        reload: "yaml",
        unit: "秒",
      },
      {
        key: "probe_timeout_secs",
        label: "探测超时",
        kind: "number",
        reload: "yaml",
        unit: "秒",
      },
      {
        key: "failure_threshold",
        label: "故障阈值",
        kind: "number",
        reload: "yaml",
        unit: "次",
      },
      {
        key: "backoff_base_secs",
        label: "退避基数",
        kind: "number",
        reload: "yaml",
        unit: "秒",
      },
      {
        key: "backoff_max_secs",
        label: "最大退避",
        kind: "number",
        reload: "yaml",
        unit: "秒",
      },
      {
        key: "cooldown_after_failures",
        label: "冷却触发次数",
        kind: "number",
        reload: "yaml",
        unit: "次",
      },
      {
        key: "cooldown_secs",
        label: "冷却时间",
        kind: "number",
        reload: "yaml",
        unit: "秒",
      },
    ],
  },
  {
    id: "replica",
    label: "Replica",
    configKey: "replica",
    fields: [
      {
        key: "replication_factor",
        label: "副本因子",
        kind: "number",
        reload: "yaml",
      },
      {
        key: "strategy",
        label: "复制策略",
        kind: "select",
        reload: "yaml",
        options: ["all", "rack", "custom"],
      },
    ],
  },
  {
    id: "quad_key",
    label: "QuadKey",
    configKey: "quad_key",
    fields: [
      { key: "base_level", label: "基础层级", kind: "number", reload: "hot" },
      { key: "split_level", label: "分割层级", kind: "number", reload: "hot" },
    ],
  },
];

// ============================================================
// Reload 标签样式
// ============================================================

function ReloadBadge({ kind }: { kind: ReloadKind }) {
  if (kind === "hot") {
    return (
      <span className="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-medium bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">
        热加载
      </span>
    );
  }
  if (kind === "restart") {
    return (
      <span
        className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] font-medium bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400 cursor-help"
        title="修改此配置需要重启服务才能生效"
      >
        <AlertTriangle className="w-2.5 h-2.5" />
        需重启
      </span>
    );
  }
  return (
    <span
      className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] font-medium bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400 cursor-help"
      title="配置写入 YAML 文件后需要重启才能生效"
    >
      <Info className="w-2.5 h-2.5" />
      YAML 回写
    </span>
  );
}

// ============================================================
// Toast 通知
// ============================================================

interface ToastState {
  show: boolean;
  type: "success" | "error";
  message: string;
}

// ============================================================
// ConfigPanel 主组件
// ============================================================

export function ConfigPanel() {
  const { config, configLoading, configError, fetchConfig, updateConfig } =
    useClusterStore();
  const [activeSection, setActiveSection] = useState("master");
  const [formValues, setFormValues] = useState<AllConfigs | null>(null);
  const [saving, setSaving] = useState(false);
  const [toast, setToast] = useState<ToastState>({
    show: false,
    type: "success",
    message: "",
  });
  const seededRef = useRef(false);

  // 初始加载
  useEffect(() => {
    fetchConfig();
  }, [fetchConfig]);

  // 仅首次获取配置后初始化表单
  useEffect(() => {
    if (config && !seededRef.current) {
      setFormValues(JSON.parse(JSON.stringify(config)));
      seededRef.current = true;
    }
  }, [config]);

  const showToast = useCallback(
    (type: "success" | "error", message: string) => {
      setToast({ show: true, type, message });
      setTimeout(() => setToast((t) => ({ ...t, show: false })), 3000);
    },
    [],
  );

  const section = SECTIONS.find((s) => s.id === activeSection)!;

  // 获取字段当前值
  function getFieldValue(key: string): string {
    if (!formValues) return "";
    const sectionData = formValues[section.configKey] as unknown as Record<
      string,
      unknown
    >;
    const val = sectionData[key];
    return val !== undefined && val !== null ? String(val) : "";
  }

  // 更新字段值
  function setFieldValue(key: string, rawValue: string) {
    if (!formValues) return;
    const field = section.fields.find((f) => f.key === key);
    const newConfig = JSON.parse(JSON.stringify(formValues)) as AllConfigs;
    const sectionData = newConfig[section.configKey] as unknown as Record<
      string,
      unknown
    >;

    if (field?.kind === "number") {
      const num = Number(rawValue);
      sectionData[key] = Number.isNaN(num) ? 0 : num;
    } else {
      sectionData[key] = rawValue;
    }

    setFormValues(newConfig);
  }

  // 重置到 API 返回的原始值
  function handleReset() {
    if (config) {
      setFormValues(JSON.parse(JSON.stringify(config)));
      setToast({ show: false, type: "success", message: "" });
    }
  }

  // 检测是否有未保存的修改
  const hasChanges = (() => {
    if (!config || !formValues) return false;
    return JSON.stringify(config) !== JSON.stringify(formValues);
  })();

  // 保存配置
  async function handleSave() {
    if (!formValues) return;
    setSaving(true);
    try {
      const ok = await updateConfig(formValues);
      if (ok) {
        showToast("success", "配置已保存并生效");
      } else {
        showToast("error", "保存失败，请重试");
      }
    } catch {
      showToast("error", "保存时发生异常");
    } finally {
      setSaving(false);
    }
  }

  // 加载中
  if (configLoading && !config) {
    return (
      <div className="flex h-full items-center justify-center bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700">
        <div className="flex flex-col items-center gap-3 text-gray-400">
          <RefreshCw className="w-6 h-6 animate-spin" />
          <span className="text-sm">加载配置中...</span>
        </div>
      </div>
    );
  }

  // 加载失败
  if (configError && !config) {
    return (
      <div className="flex h-full items-center justify-center bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700">
        <div className="flex flex-col items-center gap-3 text-red-500">
          <XCircle className="w-8 h-8" />
          <span className="text-sm font-medium">加载配置失败</span>
          <span className="text-xs text-gray-400">{configError}</span>
          <button
            onClick={() => fetchConfig()}
            className="px-3 py-1.5 text-xs bg-indigo-500 text-white rounded-lg hover:bg-indigo-600 transition-colors"
          >
            重试
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col h-full bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-100 dark:border-gray-700">
      {/* Toast */}
      {toast.show && (
        <div
          className={`absolute top-4 right-4 z-50 flex items-center gap-2 px-4 py-2.5 rounded-lg shadow-lg text-sm transition-all ${
            toast.type === "success"
              ? "bg-green-500 text-white"
              : "bg-red-500 text-white"
          }`}
          style={{ animation: "fadeIn 0.2s ease-out" }}
        >
          {toast.type === "success" ? (
            <CheckCircle2 className="w-4 h-4" />
          ) : (
            <XCircle className="w-4 h-4" />
          )}
          {toast.message}
        </div>
      )}

      {/* 标题栏 */}
      <div className="flex items-center justify-between px-4 py-2.5 border-b border-gray-100 dark:border-gray-700 flex-shrink-0">
        <div className="flex items-center gap-2">
          <Settings className="w-4 h-4 text-indigo-500" />
          <span className="font-semibold text-sm text-gray-700 dark:text-gray-300">
            配置管理
          </span>
        </div>
        <button
          onClick={() => fetchConfig()}
          className="p-1.5 rounded-lg text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700"
          title="刷新"
        >
          <RefreshCw
            className={`w-3.5 h-3.5 ${configLoading ? "animate-spin" : ""}`}
          />
        </button>
      </div>

      {/* 主体：左侧导航 + 右侧表单 */}
      <div className="flex flex-1 overflow-hidden relative">
        {/* 左侧导航 */}
        <div className="w-36 lg:w-44 flex-shrink-0 border-r border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/30 overflow-y-auto">
          <nav className="py-2">
            {SECTIONS.map((s) => {
              const isActive = activeSection === s.id;
              return (
                <button
                  key={s.id}
                  onClick={() => setActiveSection(s.id)}
                  className={`w-full text-left px-3 py-2 text-sm transition-colors ${
                    isActive
                      ? "bg-indigo-50 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400 border-r-2 border-indigo-500 font-medium"
                      : "text-gray-600 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800/50"
                  }`}
                >
                  {s.label}
                </button>
              );
            })}
          </nav>
        </div>

        {/* 右侧表单 */}
        <div className="flex-1 overflow-y-auto p-4 lg:p-6">
          <div className="max-w-lg">
            <h3 className="text-base font-semibold text-gray-800 dark:text-gray-200 mb-4">
              {section.label} 配置
            </h3>

            <div className="space-y-4">
              {section.fields.map((field) => (
                <div key={field.key} className="flex flex-col gap-1.5">
                  {/* 标签行 */}
                  <div className="flex items-center gap-2">
                    <label className="text-xs font-medium text-gray-600 dark:text-gray-400">
                      {field.label}
                    </label>
                    <span className="font-mono text-[10px] text-gray-400 bg-gray-100 dark:bg-gray-700 px-1.5 py-0.5 rounded">
                      {field.key}
                    </span>
                    <ReloadBadge kind={field.reload} />
                  </div>

                  {/* 输入控件 */}
                  {field.kind === "select" ? (
                    <select
                      value={getFieldValue(field.key)}
                      onChange={(e) => setFieldValue(field.key, e.target.value)}
                      className="w-full px-3 py-2 text-sm border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-800 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition-colors"
                    >
                      {(field.options || []).map((opt) => (
                        <option key={opt} value={opt}>
                          {opt}
                        </option>
                      ))}
                    </select>
                  ) : (
                    <div className="relative">
                      <input
                        type={field.kind === "number" ? "number" : "text"}
                        value={getFieldValue(field.key)}
                        onChange={(e) =>
                          setFieldValue(field.key, e.target.value)
                        }
                        className="w-full px-3 py-2 text-sm border border-gray-200 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 text-gray-800 dark:text-gray-200 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent transition-colors pr-12"
                      />
                      {field.unit && (
                        <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-gray-400 pointer-events-none">
                          {field.unit}
                        </span>
                      )}
                    </div>
                  )}

                  {/* 提示文本 */}
                  {field.reload === "restart" && (
                    <p className="text-[10px] text-red-500/70 flex items-center gap-1">
                      <AlertTriangle className="w-2.5 h-2.5" />
                      修改此配置需要重启服务
                    </p>
                  )}
                  {field.reload === "yaml" && (
                    <p className="text-[10px] text-amber-500/70 flex items-center gap-1">
                      <Info className="w-2.5 h-2.5" />
                      配置将写入 YAML 文件，需重启后生效
                    </p>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* 底部操作栏 */}
      <div className="flex items-center justify-between px-4 py-2.5 border-t border-gray-100 dark:border-gray-700 bg-gray-50 dark:bg-gray-900/30 flex-shrink-0">
        <div className="flex items-center gap-2">
          {hasChanges && (
            <span className="text-xs text-amber-600 dark:text-amber-400 flex items-center gap-1">
              <Info className="w-3 h-3" />
              有未保存的修改
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={handleReset}
            disabled={!hasChanges || saving}
            className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded-lg text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-gray-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            <RotateCcw className="w-3.5 h-3.5" />
            重置
          </button>
          <button
            onClick={handleSave}
            disabled={!hasChanges || saving}
            className="flex items-center gap-1.5 px-4 py-1.5 text-xs font-medium rounded-lg bg-indigo-500 text-white hover:bg-indigo-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            <Save className="w-3.5 h-3.5" />
            {saving ? "保存中..." : "保存"}
          </button>
        </div>
      </div>
    </div>
  );
}
