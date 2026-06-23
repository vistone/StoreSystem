use crate::logger::{LogCategory, LogLevel, LogQuery, LogStore};
use crate::master::MasterNode;
use crate::pending_store::PendingStore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use warp::{http::StatusCode, Filter, Rejection, Reply};

// ============================================================
// Master 管理 RESTful API
// 为前端管理界面提供数据接口
// ============================================================

/// 管理 API 上下文
pub struct AdminContext {
    pub master: Arc<MasterNode>,
    pub log_store: Arc<LogStore>,
    pub pending_store: Arc<PendingStore>,
}

impl AdminContext {
    pub fn new(master: Arc<MasterNode>, log_store: LogStore, pending_store: Arc<PendingStore>) -> Self {
        Self {
            master,
            log_store: Arc::new(log_store),
            pending_store,
        }
    }
}

// ============================================================
// 响应模型
// ============================================================

#[derive(Debug, Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
}

impl<T: Serialize> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}

/// 集群概览
#[derive(Debug, Serialize)]
pub struct ClusterOverview {
    pub total_workers: usize,
    pub alive_workers: usize,
    pub dead_workers: usize,
    pub total_storage_bytes: u64,
    pub used_storage_bytes: u64,
    pub total_memory_bytes: u64,
    pub used_memory_bytes: u64,
    pub avg_cpu_usage: f64,
    pub total_logs_today: i64,
    pub unread_logs: i64,
    pub error_logs_today: i64,
}

/// Worker 节点信息（前端展示用）
#[derive(Debug, Serialize)]
pub struct WorkerNodeInfo {
    pub worker_id: String,
    pub address: String,
    pub weight: i32,
    pub alive: bool,
    pub last_heartbeat: i64,
    pub storage_used_bytes: u64,
    pub storage_capacity_bytes: u64,
    pub storage_usage_ratio: f64,
    pub disk_health: String,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_usage_ratio: f64,
    pub cpu_usage_ratio: f64,
    pub cpu_cores: u32,
    pub active_connections: u32,
    pub tags: std::collections::HashMap<String, String>,
    // ---- 写入统计（v0.3.0 新增） ----
    pub total_put_count: u64,
    pub total_put_bytes: u64,
    pub flushed_count: u64,
    pub flushed_bytes: u64,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub write_rate_per_sec: f64,
    pub write_bytes_per_sec: f64,
}

impl From<crate::master::WorkerInfo> for WorkerNodeInfo {
    fn from(w: crate::master::WorkerInfo) -> Self {
        Self {
            worker_id: w.worker_id,
            address: w.address,
            weight: w.weight,
            alive: w.alive,
            last_heartbeat: w.last_heartbeat,
            storage_used_bytes: w.storage_used_bytes,
            storage_capacity_bytes: w.storage_capacity_bytes,
            storage_usage_ratio: w.storage_usage_ratio,
            disk_health: w.disk_health,
            memory_used_bytes: w.memory_used_bytes,
            memory_total_bytes: w.memory_total_bytes,
            memory_usage_ratio: w.memory_usage_ratio,
            cpu_usage_ratio: w.cpu_usage_ratio,
            cpu_cores: w.cpu_cores,
            active_connections: w.active_connections,
            tags: w.tags,
            total_put_count: w.total_put_count,
            total_put_bytes: w.total_put_bytes,
            flushed_count: w.flushed_count,
            flushed_bytes: w.flushed_bytes,
            pending_count: w.pending_count,
            pending_bytes: w.pending_bytes,
            write_rate_per_sec: w.write_rate_per_sec,
            write_bytes_per_sec: w.write_bytes_per_sec,
        }
    }
}

/// 日志条目（前端展示用）
#[derive(Debug, Serialize)]
pub struct LogEntryResponse {
    pub id: i64,
    pub worker_id: String,
    pub level: String,
    pub category: String,
    pub message: String,
    pub detail_json: Option<String>,
    pub timestamp: String,
    pub acknowledged: bool,
}

/// 日志统计响应
#[derive(Debug, Serialize)]
pub struct LogStatsResponse {
    pub total: i64,
    pub unread: i64,
    pub errors: i64,
    pub today: i64,
    pub by_worker: Vec<(String, i64)>,
}

/// 路由规则响应
#[derive(Debug, Serialize)]
pub struct RouteRuleResponse {
    pub key_prefix: String,
    pub worker_id: String,
    pub priority: i32,
    pub created_at: String,
}

/// 全量配置响应
#[derive(Debug, Serialize, Deserialize)]
pub struct AllConfigs {
    pub master: MasterConfigSection,
    pub worker: WorkerConfigSection,
    pub pending: PendingConfigSection,
    pub guardian: GuardianConfigSection,
    pub replica: ReplicaConfigSection,
    pub quad_key: QuadKeyConfigSection,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MasterConfigSection {
    pub heartbeat_timeout_secs: u64,
    pub cleanup_interval_secs: u64,
    pub max_message_size: usize,
    pub protocol: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkerConfigSection {
    pub cache_size: usize,
    pub flush_interval_ms: u64,
    pub heartbeat_interval_secs: u64,
    pub weight: i32,
    pub kv_ext: String,
    pub meta_ext: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PendingConfigSection {
    pub gc_interval_secs: u64,
    pub flush_timeout_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GuardianConfigSection {
    pub probe_interval_secs: u64,
    pub probe_timeout_secs: u64,
    pub failure_threshold: u32,
    pub backoff_base_secs: u64,
    pub backoff_max_secs: u64,
    pub cooldown_after_failures: u32,
    pub cooldown_secs: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReplicaConfigSection {
    pub replication_factor: i32,
    pub strategy: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuadKeyConfigSection {
    pub base_level: u32,
    pub split_level: u32,
}

/// Pending 统计响应
#[derive(Debug, Serialize)]
pub struct PendingStats {
    pub regions: HashMap<String, PendingRegionStat>,
}

#[derive(Debug, Serialize)]
pub struct PendingRegionStat {
    pub count: usize,
    pub bytes: u64,
}

// ============================================================
// 错误处理
// ============================================================

#[derive(Debug)]
struct AdminReject(String);

impl warp::reject::Reject for AdminReject {}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, Rejection> {
    if let Some(admin_err) = err.find::<AdminReject>() {
        Ok(warp::reply::with_status(
            warp::reply::json(&ApiResponse::<()>::err(&admin_err.0)),
            StatusCode::BAD_REQUEST,
        ))
    } else {
        Ok(warp::reply::with_status(
            warp::reply::json(&ApiResponse::<()>::err("Internal Server Error")),
            StatusCode::INTERNAL_SERVER_ERROR,
        ))
    }
}

// ============================================================
// CORS
// ============================================================

fn cors() -> warp::cors::Cors {
    warp::cors()
        .allow_any_origin()
        .allow_methods(vec!["GET", "POST", "DELETE", "PUT", "OPTIONS"])
        .allow_headers(vec!["Content-Type", "Authorization", "X-Requested-With"])
        .build()
}

// ============================================================
// 查询参数
// ============================================================

#[derive(Debug, Deserialize)]
pub struct LogQueryParams {
    pub worker_id: Option<String>,
    pub level: Option<String>,
    pub category: Option<String>,
    pub keyword: Option<String>,
    pub unread_only: Option<bool>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

// ============================================================
// 启动管理 API 服务
// ============================================================

pub async fn start_admin_api(ctx: AdminContext, port: u16) {
    let ctx = Arc::new(ctx);

    // GET /api/v1/overview - 集群概览
    let overview_ctx = ctx.clone();
    let overview_route = warp::path!("api" / "v1" / "overview")
        .and(warp::get())
        .and(warp::any().map(move || overview_ctx.clone()))
        .and_then(handle_overview);

    // GET /api/v1/workers - Worker 列表
    let workers_ctx = ctx.clone();
    let workers_route = warp::path!("api" / "v1" / "workers")
        .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(warp::any().map(move || workers_ctx.clone()))
        .and_then(handle_list_workers);

    // GET /api/v1/workers/:id - Worker 详情
    let worker_detail_ctx = ctx.clone();
    let worker_detail_route = warp::path!("api" / "v1" / "workers" / String)
        .and(warp::get())
        .and(warp::any().map(move || worker_detail_ctx.clone()))
        .and_then(handle_worker_detail);

    // GET /api/v1/logs - 日志查询
    let logs_ctx = ctx.clone();
    let logs_route = warp::path!("api" / "v1" / "logs")
        .and(warp::get())
        .and(warp::query::<LogQueryParams>())
        .and(warp::any().map(move || logs_ctx.clone()))
        .and_then(handle_query_logs);

    // GET /api/v1/logs/stats - 日志统计
    let log_stats_ctx = ctx.clone();
    let log_stats_route = warp::path!("api" / "v1" / "logs" / "stats")
        .and(warp::get())
        .and(warp::any().map(move || log_stats_ctx.clone()))
        .and_then(handle_log_stats);

    // GET /api/v1/logs/errors - 最近错误
    let errors_ctx = ctx.clone();
    let errors_route = warp::path!("api" / "v1" / "logs" / "errors")
        .and(warp::get())
        .and(warp::query::<std::collections::HashMap<String, String>>())
        .and(warp::any().map(move || errors_ctx.clone()))
        .and_then(handle_recent_errors);

    // POST /api/v1/logs/:id/ack - 标记日志已读
    let ack_ctx = ctx.clone();
    let ack_route = warp::path!("api" / "v1" / "logs" / i64 / "ack")
        .and(warp::post())
        .and(warp::any().map(move || ack_ctx.clone()))
        .and_then(handle_acknowledge_log);

    // POST /api/v1/logs/ack-all - 标记所有日志已读
    let ack_all_ctx = ctx.clone();
    let ack_all_route = warp::path!("api" / "v1" / "logs" / "ack-all")
        .and(warp::post())
        .and(warp::any().map(move || ack_all_ctx.clone()))
        .and_then(handle_acknowledge_all);

    // GET /api/v1/routes - 路由规则
    let routes_ctx = ctx.clone();
    let routes_route = warp::path!("api" / "v1" / "routes")
        .and(warp::get())
        .and(warp::any().map(move || routes_ctx.clone()))
        .and_then(handle_list_routes);

    // GET /api/v1/health - 健康检查
    let health_ctx = ctx.clone();
    let health_route = warp::path!("api" / "v1" / "health")
        .and(warp::get())
        .and(warp::any().map(move || health_ctx.clone()))
        .and_then(handle_health_check);

    // GET /api/v1/config - 获取全量配置
    let get_config_ctx = ctx.clone();
    let get_config_route = warp::path!("api" / "v1" / "config")
        .and(warp::get())
        .and(warp::any().map(move || get_config_ctx.clone()))
        .and_then(handle_get_config);

    // PUT /api/v1/config - 更新配置
    let put_config_ctx = ctx.clone();
    let put_config_route = warp::path!("api" / "v1" / "config")
        .and(warp::put())
        .and(warp::body::json())
        .and(warp::any().map(move || put_config_ctx.clone()))
        .and_then(handle_put_config);

    // GET /api/v1/pending - Pending 统计
    let pending_ctx = ctx.clone();
    let pending_route = warp::path!("api" / "v1" / "pending")
        .and(warp::get())
        .and(warp::any().map(move || pending_ctx.clone()))
        .and_then(handle_get_pending);

    // DELETE /api/v1/pending/:region - 清空 region pending
    let clear_pending_ctx = ctx.clone();
    let clear_pending_route = warp::path!("api" / "v1" / "pending" / String)
        .and(warp::delete())
        .and(warp::any().map(move || clear_pending_ctx.clone()))
        .and_then(handle_clear_pending);

    let routes = overview_route
        .or(workers_route)
        .or(worker_detail_route)
        .or(logs_route)
        .or(log_stats_route)
        .or(errors_route)
        .or(ack_route)
        .or(ack_all_route)
        .or(routes_route)
        .or(health_route)
        .or(get_config_route)
        .or(put_config_route)
        .or(pending_route)
        .or(clear_pending_route)
        .with(cors())
        .recover(handle_rejection);

    println!("📊 Master Admin API running on http://0.0.0.0:{}", port);
    warp::serve(routes).run(([0, 0, 0, 0], port)).await;
}

// ============================================================
// Handler 实现
// ============================================================

async fn handle_overview(ctx: Arc<AdminContext>) -> Result<impl Reply, Rejection> {
    let workers = ctx.master.list_workers(false).await;

    let total_workers = workers.len();
    let alive_workers = workers.iter().filter(|w| w.alive).count();
    let dead_workers = total_workers - alive_workers;

    let total_storage_bytes: u64 = workers.iter().map(|w| w.storage_capacity_bytes).sum();
    let used_storage_bytes: u64 = workers.iter().map(|w| w.storage_used_bytes).sum();
    let total_memory_bytes: u64 = workers.iter().map(|w| w.memory_total_bytes).sum();
    let used_memory_bytes: u64 = workers.iter().map(|w| w.memory_used_bytes).sum();

    let avg_cpu_usage = if alive_workers > 0 {
        workers
            .iter()
            .filter(|w| w.alive)
            .map(|w| w.cpu_usage_ratio)
            .sum::<f64>()
            / alive_workers as f64
    } else {
        0.0
    };

    let log_stats = ctx.log_store.get_log_stats().ok();

    let overview = ClusterOverview {
        total_workers,
        alive_workers,
        dead_workers,
        total_storage_bytes,
        used_storage_bytes,
        total_memory_bytes,
        used_memory_bytes,
        avg_cpu_usage,
        total_logs_today: log_stats.as_ref().map(|s| s.today).unwrap_or(0),
        unread_logs: log_stats.as_ref().map(|s| s.unread).unwrap_or(0),
        error_logs_today: log_stats.as_ref().map(|s| s.errors).unwrap_or(0),
    };

    Ok(warp::reply::json(&ApiResponse::ok(overview)))
}

async fn handle_list_workers(
    params: std::collections::HashMap<String, String>,
    ctx: Arc<AdminContext>,
) -> Result<impl Reply, Rejection> {
    let only_alive = params.get("alive").map(|v| v == "true").unwrap_or(false);
    let workers = ctx.master.list_workers(only_alive).await;
    let worker_infos: Vec<WorkerNodeInfo> = workers.into_iter().map(WorkerNodeInfo::from).collect();
    Ok(warp::reply::json(&ApiResponse::ok(worker_infos)))
}

async fn handle_worker_detail(
    worker_id: String,
    ctx: Arc<AdminContext>,
) -> Result<impl Reply, Rejection> {
    let workers = ctx.master.list_workers(false).await;
    if let Some(worker) = workers.into_iter().find(|w| w.worker_id == worker_id) {
        Ok(warp::reply::json(&ApiResponse::ok(WorkerNodeInfo::from(
            worker,
        ))))
    } else {
        Err(warp::reject::custom(AdminReject(format!(
            "Worker {} not found",
            worker_id
        ))))
    }
}

async fn handle_query_logs(
    params: LogQueryParams,
    ctx: Arc<AdminContext>,
) -> Result<impl Reply, Rejection> {
    let query = LogQuery {
        worker_id: params.worker_id,
        level: params.level.as_ref().map(|s| LogLevel::from(s.as_str())),
        category: params
            .category
            .as_ref()
            .map(|s| LogCategory::from(s.as_str())),
        keyword: params.keyword,
        unread_only: params.unread_only.unwrap_or(false),
        limit: params.limit.unwrap_or(100),
        offset: params.offset.unwrap_or(0),
        start_time: params
            .start_time
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc)),
        end_time: params
            .end_time
            .as_deref()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&chrono::Utc)),
    };

    match ctx.log_store.query_logs(&query) {
        Ok(entries) => {
            let total = ctx.log_store.count_logs(&query).unwrap_or(0);
            let responses: Vec<LogEntryResponse> = entries
                .into_iter()
                .map(|e| LogEntryResponse {
                    id: e.id,
                    worker_id: e.worker_id,
                    level: e.level.to_string(),
                    category: e.category.to_string(),
                    message: e.message,
                    detail_json: e.detail_json,
                    timestamp: e.timestamp.to_rfc3339(),
                    acknowledged: e.acknowledged,
                })
                .collect();

            Ok(warp::reply::json(&serde_json::json!({
                "success": true,
                "data": {
                    "entries": responses,
                    "total": total,
                    "limit": query.limit,
                    "offset": query.offset,
                }
            })))
        }
        Err(e) => Ok(warp::reply::json(&ApiResponse::<()>::err(e.to_string()))),
    }
}

async fn handle_log_stats(ctx: Arc<AdminContext>) -> Result<impl Reply, Rejection> {
    match ctx.log_store.get_log_stats() {
        Ok(stats) => Ok(warp::reply::json(&ApiResponse::ok(LogStatsResponse {
            total: stats.total,
            unread: stats.unread,
            errors: stats.errors,
            today: stats.today,
            by_worker: stats.by_worker,
        }))),
        Err(e) => Ok(warp::reply::json(&ApiResponse::<()>::err(e.to_string()))),
    }
}

async fn handle_recent_errors(
    params: std::collections::HashMap<String, String>,
    ctx: Arc<AdminContext>,
) -> Result<impl Reply, Rejection> {
    let limit = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    match ctx.log_store.get_recent_errors(limit) {
        Ok(entries) => {
            let responses: Vec<LogEntryResponse> = entries
                .into_iter()
                .map(|e| LogEntryResponse {
                    id: e.id,
                    worker_id: e.worker_id,
                    level: e.level.to_string(),
                    category: e.category.to_string(),
                    message: e.message,
                    detail_json: e.detail_json,
                    timestamp: e.timestamp.to_rfc3339(),
                    acknowledged: e.acknowledged,
                })
                .collect();
            Ok(warp::reply::json(&ApiResponse::ok(responses)))
        }
        Err(e) => Ok(warp::reply::json(&ApiResponse::<()>::err(e.to_string()))),
    }
}

async fn handle_acknowledge_log(
    log_id: i64,
    ctx: Arc<AdminContext>,
) -> Result<impl Reply, Rejection> {
    match ctx.log_store.acknowledge_log(log_id) {
        Ok(true) => Ok(warp::reply::json(&ApiResponse::ok(true))),
        Ok(false) => Err(warp::reject::custom(AdminReject(format!(
            "Log {} not found",
            log_id
        )))),
        Err(e) => Ok(warp::reply::json(&ApiResponse::<()>::err(e.to_string()))),
    }
}

async fn handle_acknowledge_all(ctx: Arc<AdminContext>) -> Result<impl Reply, Rejection> {
    // 标记所有日志为已读
    match ctx.log_store.get_unread_logs(10000) {
        Ok(entries) => {
            let ids: Vec<i64> = entries.iter().map(|e| e.id).collect();
            if ids.is_empty() {
                return Ok(warp::reply::json(&ApiResponse::ok(0)));
            }
            match ctx.log_store.acknowledge_logs_batch(&ids) {
                Ok(count) => Ok(warp::reply::json(&ApiResponse::ok(count))),
                Err(e) => Ok(warp::reply::json(&ApiResponse::<()>::err(e.to_string()))),
            }
        }
        Err(e) => Ok(warp::reply::json(&ApiResponse::<()>::err(e.to_string()))),
    }
}

async fn handle_list_routes(ctx: Arc<AdminContext>) -> Result<impl Reply, Rejection> {
    let rules = ctx.master.store.list_route_rules().unwrap_or_default();
    let responses: Vec<RouteRuleResponse> = rules
        .into_iter()
        .map(|r| RouteRuleResponse {
            key_prefix: r.key_prefix,
            worker_id: r.worker_id,
            priority: r.priority,
            created_at: r.created_at.to_rfc3339(),
        })
        .collect();
    Ok(warp::reply::json(&ApiResponse::ok(responses)))
}

async fn handle_health_check(ctx: Arc<AdminContext>) -> Result<impl Reply, Rejection> {
    let workers = ctx.master.list_workers(false).await;
    let alive = workers.iter().filter(|w| w.alive).count();
    let total = workers.len();

    let health = serde_json::json!({
        "status": if alive > 0 { "healthy" } else { "degraded" },
        "alive_workers": alive,
        "total_workers": total,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    Ok(warp::reply::json(&ApiResponse::ok(health)))
}

// ============================================================
// Config Handlers
// ============================================================

async fn handle_get_config(ctx: Arc<AdminContext>) -> Result<impl Reply, Rejection> {
    let mc = &ctx.master.config;
    let wd = ctx.master.worker_defaults.as_ref();

    let config = AllConfigs {
        master: MasterConfigSection {
            heartbeat_timeout_secs: mc.heartbeat_timeout_secs,
            cleanup_interval_secs: mc.cleanup_interval_secs,
            max_message_size: ctx.master.get_max_message_size(),
            protocol: ctx.master.protocol.clone(),
        },
        worker: WorkerConfigSection {
            cache_size: wd.cache_size,
            flush_interval_ms: wd.flush_interval_ms,
            heartbeat_interval_secs: wd.heartbeat_interval_secs,
            weight: wd.weight,
            kv_ext: wd.kv_ext.clone(),
            meta_ext: wd.meta_ext.clone(),
        },
        pending: PendingConfigSection {
            gc_interval_secs: mc.pending_gc_interval_secs,
            flush_timeout_secs: mc.pending_flush_timeout_secs,
        },
        guardian: GuardianConfigSection {
            probe_interval_secs: 5,
            probe_timeout_secs: 3,
            failure_threshold: 3,
            backoff_base_secs: 1,
            backoff_max_secs: 60,
            cooldown_after_failures: 10,
            cooldown_secs: 300,
        },
        replica: ReplicaConfigSection {
            replication_factor: 2,
            strategy: "all".to_string(),
        },
        quad_key: QuadKeyConfigSection {
            base_level: wd.quad_shard.base_level,
            split_level: wd.quad_shard.split_level,
        },
    };

    Ok(warp::reply::json(&ApiResponse::ok(config)))
}

async fn handle_put_config(
    body: AllConfigs,
    ctx: Arc<AdminContext>,
) -> Result<impl Reply, Rejection> {
    // 更新集群 config 表
    let store = &ctx.master.store;
    let updates = vec![
        ("heartbeat_timeout_secs", body.master.heartbeat_timeout_secs.to_string()),
        ("cleanup_interval_secs", body.master.cleanup_interval_secs.to_string()),
        ("cache_size", body.worker.cache_size.to_string()),
        ("flush_interval_ms", body.worker.flush_interval_ms.to_string()),
        ("heartbeat_interval_secs", body.worker.heartbeat_interval_secs.to_string()),
        ("weight", body.worker.weight.to_string()),
        ("replication_factor", body.replica.replication_factor.to_string()),
        ("replication_strategy", body.replica.strategy.clone()),
        ("base_level", body.quad_key.base_level.to_string()),
        ("split_level", body.quad_key.split_level.to_string()),
    ];
    for (key, value) in &updates {
        let _ = store.set_config(key, value);
    }

    // 推送 Worker 配置热更新
    let config_json = serde_json::json!({
        "type": "config_update",
        "cache_size": body.worker.cache_size,
        "flush_interval_ms": body.worker.flush_interval_ms,
        "heartbeat_interval_secs": body.worker.heartbeat_interval_secs,
        "weight": body.worker.weight,
    });
    // 通过 ConfigBroadcaster 广播给所有 Worker
    if let Some(broadcaster) = ctx.master.config_broadcaster() {
        broadcaster.broadcast_all(&config_json.to_string());
    }

    Ok(warp::reply::json(&ApiResponse::ok(
        serde_json::json!({"updated": updates.len()}),
    )))
}

// ============================================================
// Pending Handlers
// ============================================================

async fn handle_get_pending(ctx: Arc<AdminContext>) -> Result<impl Reply, Rejection> {
    let mut regions = HashMap::new();
    for region in &["0", "1", "2", "3"] {
        let count = ctx
            .pending_store
            .list_by_status(region, &["pending", "flushing"])
            .unwrap_or_default()
            .len();
        let bytes_estimate = if count > 0 { count as u64 * 4096 } else { 0 };
        regions.insert(
            region.to_string(),
            PendingRegionStat {
                count,
                bytes: bytes_estimate,
            },
        );
    }
    Ok(warp::reply::json(&ApiResponse::ok(PendingStats { regions })))
}

async fn handle_clear_pending(
    region: String,
    ctx: Arc<AdminContext>,
) -> Result<impl Reply, Rejection> {
    // 清空指定 region 的 pending（强制 GC）
    let cleaned = ctx.pending_store.gc_done(&region, 0).unwrap_or(0);
    Ok(warp::reply::json(&ApiResponse::ok(
        serde_json::json!({"cleaned": cleaned, "region": region}),
    )))
}
