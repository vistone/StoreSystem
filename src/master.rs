use crate::error::{Result, StoreError};
use crate::grpc::proto;
use crate::master_http::WorkerHttpClient;
use crate::master_store::MasterStore;
use crate::master_ws::WorkerWsClient;
use crate::meta::ObjectMeta;
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

/// Worker 节点信息
#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub address: String,
    pub weight: i32,
    pub alive: bool,
    pub last_heartbeat: i64,
    pub storage_used_bytes: u64,
    pub storage_capacity_bytes: u64,
    pub active_connections: u32,
    pub tags: HashMap<String, String>,
    // ---- 健康监控字段 ----
    pub storage_usage_ratio: f64,
    pub disk_health: String,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_usage_ratio: f64,
    pub cpu_usage_ratio: f64,
    pub cpu_cores: u32,
    // ---- 写入统计字段（v0.3.0 新增） ----
    pub total_put_count: u64,
    pub total_put_bytes: u64,
    pub flushed_count: u64,
    pub flushed_bytes: u64,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub write_rate_per_sec: f64,
    pub write_bytes_per_sec: f64,
}

/// Worker 心跳数据载体（替代 19 个散列参数）
#[derive(Debug, Default, Clone)]
pub struct HeartbeatPayload {
    pub storage_used_bytes: u64,
    pub storage_capacity_bytes: u64,
    pub active_connections: u32,
    pub storage_usage_ratio: f64,
    pub disk_health: String,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_usage_ratio: f64,
    pub cpu_usage_ratio: f64,
    pub cpu_cores: u32,
    pub total_put_count: u64,
    pub total_put_bytes: u64,
    pub flushed_count: u64,
    pub flushed_bytes: u64,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub write_rate_per_sec: f64,
    pub write_bytes_per_sec: f64,
}

/// Master 节点配置
#[derive(Debug, Clone)]
pub struct MasterConfig {
    /// Master 监听地址
    pub listen_addr: String,
    /// Meta 数据库路径（存储 Worker 信息和路由表）
    pub meta_path: String,
    /// Worker 心跳超时（秒），超过此时间未收到心跳视为宕机
    pub heartbeat_timeout_secs: u64,
    /// 清理宕机 Worker 的间隔（秒）
    pub cleanup_interval_secs: u64,
}

impl Default for MasterConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:50051".to_string(),
            meta_path: "master_data/master.db".to_string(),
            heartbeat_timeout_secs: 30,
            cleanup_interval_secs: 60,
        }
    }
}

/// Master 节点：管理 Worker 注册、路由、对外提供服务
pub struct MasterNode {
    pub config: MasterConfig,
    /// 通讯协议: "grpc" | "restful" | "ws" | "both"
    pub protocol: String,
    /// Master 集群元数据库（SQLite 持久化）
    pub store: MasterStore,
    /// Worker 信息表（内存缓存，启动时从 SQLite 加载）
    workers: Arc<RwLock<HashMap<String, WorkerInfo>>>,
    /// Worker gRPC 客户端缓存（避免频繁创建连接）
    worker_clients: Arc<
        DashMap<
            String,
            proto::worker_service_client::WorkerServiceClient<tonic::transport::Channel>,
        >,
    >,
    /// Worker HTTP 客户端缓存
    http_clients: Arc<DashMap<String, WorkerHttpClient>>,
    /// Worker WS 客户端缓存
    ws_clients: Arc<DashMap<String, WorkerWsClient>>,
}

impl std::fmt::Debug for MasterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MasterNode")
            .field("config", &self.config)
            .field("protocol", &self.protocol)
            .field("workers", &self.workers)
            .finish()
    }
}

impl MasterNode {
    pub fn open(config: MasterConfig) -> Result<Self> {
        Self::open_with_protocol(config, "both")
    }

    pub fn open_with_protocol(config: MasterConfig, protocol: &str) -> Result<Self> {
        // 打开 Master 集群元数据库（SQLite 持久化）
        let store = MasterStore::open(&config.meta_path)?;

        // 从 SQLite 加载 Worker 信息到内存缓存
        let mut workers_map = HashMap::new();
        if let Ok(registrations) = store.list_workers(false) {
            for reg in registrations {
                // 解析 tags_json -> HashMap
                let tags: HashMap<String, String> =
                    serde_json::from_str(&reg.tags_json).unwrap_or_default();
                let last_heartbeat = reg.last_heartbeat.timestamp();

                workers_map.insert(
                    reg.worker_id.clone(),
                    WorkerInfo {
                        worker_id: reg.worker_id,
                        address: reg.address,
                        weight: reg.weight,
                        alive: reg.alive,
                        last_heartbeat,
                        storage_used_bytes: reg.storage_used_bytes,
                        storage_capacity_bytes: reg.storage_capacity_bytes,
                        active_connections: reg.active_connections,
                        tags,
                        storage_usage_ratio: reg.storage_usage_ratio,
                        disk_health: reg.disk_health,
                        memory_used_bytes: reg.memory_used_bytes,
                        memory_total_bytes: reg.memory_total_bytes,
                        memory_usage_ratio: reg.memory_usage_ratio,
                        cpu_usage_ratio: reg.cpu_usage_ratio,
                        cpu_cores: reg.cpu_cores,
                        total_put_count: reg.total_put_count,
                        total_put_bytes: reg.total_put_bytes,
                        flushed_count: reg.flushed_count,
                        flushed_bytes: reg.flushed_bytes,
                        pending_count: reg.pending_count,
                        pending_bytes: reg.pending_bytes,
                        write_rate_per_sec: reg.write_rate_per_sec,
                        write_bytes_per_sec: reg.write_bytes_per_sec,
                    },
                );
            }
        }

        println!("[Master] 已加载 {} 个 Worker 注册信息", workers_map.len());

        Ok(Self {
            config,
            protocol: protocol.to_string(),
            store,
            workers: Arc::new(RwLock::new(workers_map)),
            worker_clients: Arc::new(DashMap::new()),
            http_clients: Arc::new(DashMap::new()),
            ws_clients: Arc::new(DashMap::new()),
        })
    }

    /// 注册 Worker（同时写入内存缓存和 SQLite 持久化）
    pub async fn register_worker(
        &self,
        worker_id: &str,
        address: &str,
        weight: i32,
        tags: HashMap<String, String>,
    ) -> Result<()> {
        // 先写入 SQLite 持久化
        self.store
            .register_worker(worker_id, address, weight, &tags)?;

        // 再更新内存缓存
        let mut workers = self.workers.write().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        workers.insert(
            worker_id.to_string(),
            WorkerInfo {
                worker_id: worker_id.to_string(),
                address: address.to_string(),
                weight,
                alive: true,
                last_heartbeat: now,
                storage_used_bytes: 0,
                storage_capacity_bytes: 0,
                active_connections: 0,
                tags,
                storage_usage_ratio: 0.0,
                disk_health: "Unknown".to_string(),
                memory_used_bytes: 0,
                memory_total_bytes: 0,
                memory_usage_ratio: 0.0,
                cpu_usage_ratio: 0.0,
                cpu_cores: 0,
                total_put_count: 0,
                total_put_bytes: 0,
                flushed_count: 0,
                flushed_bytes: 0,
                pending_count: 0,
                pending_bytes: 0,
                write_rate_per_sec: 0.0,
                write_bytes_per_sec: 0.0,
            },
        );

        // 清除旧的客户端连接缓存
        self.worker_clients.remove(worker_id);

        Ok(())
    }

    /// 处理 Worker 心跳（同时更新内存缓存和 SQLite 持久化）
    pub async fn heartbeat(&self, worker_id: &str, p: HeartbeatPayload) -> Result<bool> {
        // 先更新 SQLite 持久化
        self.store.update_heartbeat(
            worker_id,
            p.storage_used_bytes,
            p.storage_capacity_bytes,
            p.active_connections,
            p.storage_usage_ratio,
            &p.disk_health,
            p.memory_used_bytes,
            p.memory_total_bytes,
            p.memory_usage_ratio,
            p.cpu_usage_ratio,
            p.cpu_cores,
            p.total_put_count,
            p.total_put_bytes,
            p.flushed_count,
            p.flushed_bytes,
            p.pending_count,
            p.pending_bytes,
            p.write_rate_per_sec,
            p.write_bytes_per_sec,
        )?;

        // 再更新内存缓存
        let mut workers = self.workers.write().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        if let Some(worker) = workers.get_mut(worker_id) {
            worker.alive = true;
            worker.last_heartbeat = now;
            worker.storage_used_bytes = p.storage_used_bytes;
            worker.storage_capacity_bytes = p.storage_capacity_bytes;
            worker.active_connections = p.active_connections;
            worker.storage_usage_ratio = p.storage_usage_ratio;
            worker.disk_health = p.disk_health;
            worker.memory_used_bytes = p.memory_used_bytes;
            worker.memory_total_bytes = p.memory_total_bytes;
            worker.memory_usage_ratio = p.memory_usage_ratio;
            worker.cpu_usage_ratio = p.cpu_usage_ratio;
            worker.cpu_cores = p.cpu_cores;
            worker.total_put_count = p.total_put_count;
            worker.total_put_bytes = p.total_put_bytes;
            worker.flushed_count = p.flushed_count;
            worker.flushed_bytes = p.flushed_bytes;
            worker.pending_count = p.pending_count;
            worker.pending_bytes = p.pending_bytes;
            worker.write_rate_per_sec = p.write_rate_per_sec;
            worker.write_bytes_per_sec = p.write_bytes_per_sec;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// 获取所有 Worker 列表
    pub async fn list_workers(&self, only_alive: bool) -> Vec<WorkerInfo> {
        let workers = self.workers.read().await;
        workers
            .values()
            .filter(|w| !only_alive || w.alive)
            .cloned()
            .collect()
    }

    /// 根据 key 路由到对应的 Worker（Rendezvous Hashing / HRW）
    ///
    /// 相比取模哈希，Rendezvous Hashing 的优势：
    /// Worker 增减时只有 ~1/N 的 key 受影响，而取模哈希会导致几乎所有 key 重映射。
    pub async fn route(&self, key: &str) -> Result<WorkerInfo> {
        let workers = self.workers.read().await;
        let alive_workers: Vec<&WorkerInfo> = workers.values().filter(|w| w.alive).collect();

        if alive_workers.is_empty() {
            return Err(StoreError::InvalidArgument(
                "没有可用的 Worker 节点".to_string(),
            ));
        }

        // Rendezvous Hashing: 对每个 Worker 计算 hash(key + '\0' + worker_id)
        // 选择哈希值最大的 Worker。先按 worker_id 排序保证迭代顺序确定性
        // （防止 HashMap 随机种子导致两 worker 哈希相等时选中不同 worker）
        let mut candidates: Vec<&WorkerInfo> = alive_workers.into_iter().collect();
        candidates.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));
        let best = candidates
            .iter()
            .max_by_key(|w| {
                let input = format!("{}\0{}", key, w.worker_id);
                seahash::hash(input.as_bytes())
            })
            .unwrap();

        Ok((*best).clone())
    }

    /// 路由到主副本 + 备副本（Rendezvous Hashing 排序取前 2）
    ///
    /// 返回 (primary, optional_secondary)。
    /// secondary 用于异步副本写入，提升高可用性。
    pub async fn route_with_replicas(
        &self,
        key: &str,
    ) -> Result<(WorkerInfo, Option<WorkerInfo>)> {
        let workers = self.workers.read().await;
        let alive_workers: Vec<&WorkerInfo> = workers.values().filter(|w| w.alive).collect();

        if alive_workers.is_empty() {
            return Err(StoreError::InvalidArgument(
                "没有可用的 Worker 节点".to_string(),
            ));
        }

        let mut scored: Vec<(u64, &WorkerInfo)> = alive_workers
            .iter()
            .map(|w| {
                let input = format!("{}\0{}", key, w.worker_id);
                (seahash::hash(input.as_bytes()), *w)
            })
            .collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0)); // 降序：hash 最大在前

        let primary = scored[0].1.clone();
        let secondary = if scored.len() > 1 {
            Some(scored[1].1.clone())
        } else {
            None
        };

        Ok((primary, secondary))
    }

    /// 获取 Worker 的 gRPC 客户端
    async fn get_worker_client(
        &self,
        address: &str,
    ) -> Result<proto::worker_service_client::WorkerServiceClient<tonic::transport::Channel>> {
        // 先从缓存查找
        if let Some(client) = self.worker_clients.get(address) {
            return Ok(client.clone());
        }

        // 创建新连接
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{}", address))
            .map_err(|e| StoreError::InvalidArgument(format!("无效的 Worker 地址: {}", e)))?
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5));

        let client = proto::worker_service_client::WorkerServiceClient::connect(endpoint)
            .await
            .map_err(|e| StoreError::InvalidArgument(format!("连接 Worker 失败: {}", e)))?
            .max_decoding_message_size(256 * 1024 * 1024)
            .max_encoding_message_size(256 * 1024 * 1024);

        self.worker_clients
            .insert(address.to_string(), client.clone());
        Ok(client)
    }

    /// 获取 Worker 的 HTTP 客户端
    fn get_http_client(&self, address: &str) -> WorkerHttpClient {
        if let Some(client) = self.http_clients.get(address) {
            return client.clone();
        }
        let client = WorkerHttpClient::new(address);
        self.http_clients
            .insert(address.to_string(), client.clone());
        client
    }

    /// 获取 Worker 的 WS 客户端
    fn get_ws_client(&self, address: &str) -> WorkerWsClient {
        if let Some(client) = self.ws_clients.get(address) {
            return client.clone();
        }
        let client = WorkerWsClient::new(address);
        self.ws_clients.insert(address.to_string(), client.clone());
        client
    }

    /// 清理超时的 Worker（标记为宕机）
    pub async fn cleanup_dead_workers(&self) {
        let mut workers = self.workers.write().await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let timeout = self.config.heartbeat_timeout_secs as i64;

        for worker in workers.values_mut() {
            if worker.alive && (now - worker.last_heartbeat) > timeout {
                worker.alive = false;
                eprintln!("[Master] Worker {} 心跳超时，标记为宕机", worker.worker_id);
            }
        }
    }

    // ============================================================
    // 对外存储接口（客户端 -> Master -> Worker）
    // 根据 protocol 配置选择通讯方式
    // ============================================================

    pub async fn put(
        &self,
        key: String,
        value: Bytes,
        content_type: Option<String>,
        tags: Option<serde_json::Value>,
    ) -> Result<ObjectMeta> {
        let worker = self.route(&key).await?;

        match self.protocol.as_str() {
            "restful" => {
                let client = self.get_http_client(&worker.address);
                let tags_str = tags.as_ref().map(|t| t.to_string());
                client
                    .put(&key, value, content_type.as_deref(), tags_str.as_deref())
                    .await
            }
            "ws" => {
                let client = self.get_ws_client(&worker.address);
                let tags_str = tags.as_ref().map(|t| t.to_string());
                client
                    .put(&key, value, content_type.as_deref(), tags_str.as_deref())
                    .await
            }
            _ => {
                // gRPC: 使用副本路由，同步写主，异步写备
                let (primary, secondary) = self.route_with_replicas(&key).await?;

                let now = Utc::now();
                let meta = ObjectMeta {
                    key: key.clone(),
                    size: value.len() as u64,
                    created_at: now,
                    updated_at: now,
                    content_type: content_type.clone(),
                    tags: tags.clone(),
                    checksum: None,
                    storage_node: Some(primary.worker_id.clone()),
                };

                let request = tonic::Request::new(proto::PutRequest {
                    key: meta.key.clone(),
                    value: value.to_vec(),
                    content_type: meta.content_type.clone().unwrap_or_default(),
                    tags: meta
                        .tags
                        .as_ref()
                        .map(|t| t.to_string())
                        .unwrap_or_default(),
                });

                // 同步写入主副本（失败则 fallback 到备副本）
                let put_result = {
                    let mut client = match self.get_worker_client(&primary.address).await {
                        Ok(c) => c,
                        Err(_) => {
                            // 主副本连接失败，尝试备副本
                            if let Some(ref sec) = secondary {
                                self.get_worker_client(&sec.address).await?
                            } else {
                                return Err(StoreError::InvalidArgument(
                                    "主副本不可达且无备副本".to_string(),
                                ));
                            }
                        }
                    };
                    client.put(request).await
                };

                match put_result {
                    Ok(_) => {}
                    Err(e) => {
                        // 主副本写入失败，尝试备副本
                        if let Some(ref sec) = secondary {
                            let mut client = self.get_worker_client(&sec.address).await?;
                            client
                                .put(tonic::Request::new(proto::PutRequest {
                                    key: meta.key.clone(),
                                    value: value.to_vec(),
                                    content_type: meta.content_type.clone().unwrap_or_default(),
                                    tags: meta
                                        .tags
                                        .as_ref()
                                        .map(|t| t.to_string())
                                        .unwrap_or_default(),
                                }))
                                .await
                                .map_err(|e2| {
                                    StoreError::InvalidArgument(format!(
                                        "副本写入失败(主+备均失败, 主: {}, 备: {})",
                                        e, e2
                                    ))
                                })?;
                        } else {
                            return Err(StoreError::InvalidArgument(format!(
                                "主副本写入失败且无备副本: {}",
                                e
                            )));
                        }
                    }
                }

                // 异步写入备副本（火后即忘，失败不影响主流程）
                if let Some(sec) = secondary {
                    let sec_addr = sec.address.clone();
                    let sec_request = tonic::Request::new(proto::PutRequest {
                        key: meta.key.clone(),
                        value: value.to_vec(),
                        content_type: meta.content_type.clone().unwrap_or_default(),
                        tags: meta
                            .tags
                            .as_ref()
                            .map(|t| t.to_string())
                            .unwrap_or_default(),
                    });
                    let _max_msg = self.config.heartbeat_timeout_secs; // 复用为超时秒数
                    tokio::spawn(async move {
                        let endpoint = match tonic::transport::Endpoint::from_shared(
                            format!("http://{}", sec_addr),
                        ) {
                            Ok(e) => e
                                .timeout(Duration::from_secs(30))
                                .connect_timeout(Duration::from_secs(3)),
                            Err(_) => return,
                        };
                        if let Ok(mut c) =
                            proto::worker_service_client::WorkerServiceClient::connect(endpoint)
                                .await
                        {
                            let _ = c.put(sec_request).await;
                        }
                    });
                }

                Ok(meta)
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<(Bytes, ObjectMeta)> {
        let worker = self.route(key).await?;

        match self.protocol.as_str() {
            "restful" => {
                let client = self.get_http_client(&worker.address);
                client.get(key).await
            }
            "ws" => {
                let client = self.get_ws_client(&worker.address);
                client.get(key).await
            }
            _ => {
                let (primary, secondary) = self.route_with_replicas(key).await?;

                // 尝试主副本
                let result = {
                    let mut client = self.get_worker_client(&primary.address).await?;
                    let request = tonic::Request::new(proto::GetRequest {
                        key: key.to_string(),
                    });
                    client.get(request).await
                };

                let response = match result {
                    Ok(r) => r,
                    Err(e) => {
                        // 主副本失败，尝试备副本
                        if let Some(sec) = &secondary {
                            let mut client = self.get_worker_client(&sec.address).await?;
                            let request =
                                tonic::Request::new(proto::GetRequest { key: key.to_string() });
                            client.get(request).await.map_err(|_| {
                                StoreError::InvalidArgument(format!(
                                    "Worker 读取失败(主+备均失败): {}",
                                    e
                                ))
                            })?
                        } else {
                            return Err(StoreError::InvalidArgument(format!(
                                "Worker 读取失败(无备副本): {}",
                                e
                            )));
                        }
                    }
                };

                let resp = response.into_inner();
                let meta_proto = resp
                    .meta
                    .ok_or_else(|| StoreError::MetaNotFound(key.to_string()))?;

                let meta = ObjectMeta {
                    key: meta_proto.key,
                    size: meta_proto.size,
                    created_at: chrono::DateTime::parse_from_rfc3339(&meta_proto.created_at)
                        .map_err(|e| StoreError::InvalidArgument(format!("时间解析失败: {}", e)))?
                        .with_timezone(&Utc),
                    updated_at: chrono::DateTime::parse_from_rfc3339(&meta_proto.updated_at)
                        .map_err(|e| StoreError::InvalidArgument(format!("时间解析失败: {}", e)))?
                        .with_timezone(&Utc),
                    content_type: if meta_proto.content_type.is_empty() {
                        None
                    } else {
                        Some(meta_proto.content_type)
                    },
                    tags: if meta_proto.tags.is_empty() {
                        None
                    } else {
                        Some(serde_json::from_str(&meta_proto.tags).map_err(|e| {
                            StoreError::InvalidArgument(format!("Tags 解析失败: {}", e))
                        })?)
                    },
                    checksum: None,
                    storage_node: None,
                };

                Ok((Bytes::from(resp.value), meta))
            }
        }
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let worker = self.route(key).await?;

        match self.protocol.as_str() {
            "restful" => {
                let client = self.get_http_client(&worker.address);
                client.delete(key).await
            }
            "ws" => {
                let client = self.get_ws_client(&worker.address);
                client.delete(key).await
            }
            _ => {
                let mut client = self.get_worker_client(&worker.address).await?;

                let request = tonic::Request::new(proto::DeleteRequest {
                    key: key.to_string(),
                });

                client
                    .delete(request)
                    .await
                    .map_err(|e| StoreError::InvalidArgument(format!("Worker 删除失败: {}", e)))?;

                Ok(())
            }
        }
    }

    pub async fn exists(&self, key: &str) -> Result<bool> {
        let worker = self.route(key).await?;

        match self.protocol.as_str() {
            "restful" => {
                let client = self.get_http_client(&worker.address);
                client.exists(key).await
            }
            "ws" => {
                let client = self.get_ws_client(&worker.address);
                client.exists(key).await
            }
            _ => {
                let mut client = self.get_worker_client(&worker.address).await?;

                let request = tonic::Request::new(proto::ExistsRequest {
                    key: key.to_string(),
                });

                let response = client
                    .exists(request)
                    .await
                    .map_err(|e| StoreError::InvalidArgument(format!("Worker 查询失败: {}", e)))?;

                Ok(response.into_inner().exists)
            }
        }
    }

    pub async fn list(&self, prefix: &str, limit: usize) -> Result<Vec<ObjectMeta>> {
        let workers = self.list_workers(true).await;
        if workers.is_empty() {
            return Ok(vec![]);
        }

        let protocol = self.protocol.clone();
        let http_clients = self.http_clients.clone();
        let ws_clients = self.ws_clients.clone();

        // 并发查询所有存活 Worker
        let tasks: Vec<_> = workers
            .into_iter()
            .map(|worker| {
                let protocol = protocol.clone();
                let prefix = prefix.to_string();
                let http_clients = http_clients.clone();
                let ws_clients = ws_clients.clone();

                tokio::spawn(async move {
                    match protocol.as_str() {
                        "restful" => {
                            let client = if let Some(c) = http_clients.get(&worker.address) {
                                c.clone()
                            } else {
                                let c = crate::master_http::WorkerHttpClient::new(&worker.address);
                                http_clients.insert(worker.address.clone(), c.clone());
                                c
                            };
                            client.list(&prefix, limit as u32).await.ok()
                        }
                        "ws" => {
                            let client = if let Some(c) = ws_clients.get(&worker.address) {
                                c.clone()
                            } else {
                                let c = crate::master_ws::WorkerWsClient::new(&worker.address);
                                ws_clients.insert(worker.address.clone(), c.clone());
                                c
                            };
                            client.list(&prefix, limit as u32).await.ok()
                        }
                        _ => {
                            // gRPC：list 操作不频繁，每次新建连接，避免跨任务共享客户端
                            let endpoint = match tonic::transport::Endpoint::from_shared(
                                format!("http://{}", worker.address),
                            ) {
                                Ok(e) => e
                                    .timeout(Duration::from_secs(30))
                                    .connect_timeout(Duration::from_secs(5)),
                                Err(_) => return None,
                            };
                            let mut client =
                                match proto::worker_service_client::WorkerServiceClient::connect(
                                    endpoint,
                                )
                                .await
                                {
                                    Ok(c) => c
                                        .max_decoding_message_size(256 * 1024 * 1024)
                                        .max_encoding_message_size(256 * 1024 * 1024),
                                    Err(_) => return None,
                                };

                            let request = tonic::Request::new(proto::ListRequest {
                                prefix: prefix.clone(),
                                limit: limit as u32,
                            });

                            match client.list(request).await {
                                Ok(response) => {
                                    let mut metas = Vec::new();
                                    for meta_proto in response.into_inner().metas {
                                        if let Ok(meta) = proto_to_object_meta(meta_proto) {
                                            metas.push(meta);
                                        }
                                    }
                                    Some(metas)
                                }
                                Err(_) => None,
                            }
                        }
                    }
                })
            })
            .collect();

        let results = futures::future::join_all(tasks).await;

        let mut all_metas: Vec<ObjectMeta> = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for result in results {
            if let Ok(Some(metas)) = result {
                for meta in metas {
                    if seen.insert(meta.key.clone()) {
                        all_metas.push(meta);
                    }
                }
            }
        }

        all_metas.sort_by(|a, b| a.key.cmp(&b.key));
        all_metas.truncate(limit);
        Ok(all_metas)
    }

    pub async fn put_batch(
        &self,
        items: Vec<(String, Bytes, Option<String>, Option<serde_json::Value>)>,
    ) -> Result<Vec<ObjectMeta>> {
        // 按 Worker 分组
        #[allow(clippy::type_complexity)]
        let mut worker_items: HashMap<
            String,
            Vec<(String, Bytes, Option<String>, Option<serde_json::Value>)>,
        > = HashMap::new();

        for item in items {
            let worker = self.route(&item.0).await?;
            worker_items
                .entry(worker.address.clone())
                .or_default()
                .push(item);
        }

        let mut all_metas = Vec::new();

        for (address, items) in worker_items {
            let metas = match self.protocol.as_str() {
                "restful" => {
                    let client = self.get_http_client(&address);
                    client.put_batch(items).await.ok()
                }
                "ws" => {
                    let client = self.get_ws_client(&address);
                    client.put_batch(items).await.ok()
                }
                _ => {
                    let mut client = self.get_worker_client(&address).await?;

                    let batch_items: Vec<proto::BatchItem> = items
                        .iter()
                        .map(|(key, value, ct, tags)| proto::BatchItem {
                            key: key.clone(),
                            value: value.to_vec(),
                            content_type: ct.clone().unwrap_or_default(),
                            tags: tags.as_ref().map(|t| t.to_string()).unwrap_or_default(),
                        })
                        .collect();

                    let request =
                        tonic::Request::new(proto::PutBatchRequest { items: batch_items });

                    if let Ok(response) = client.put_batch(request).await {
                        let mut metas = Vec::new();
                        for meta_proto in response.into_inner().metas {
                            if let Ok(meta) = proto_to_object_meta(meta_proto) {
                                metas.push(meta);
                            }
                        }
                        Some(metas)
                    } else {
                        None
                    }
                }
            };

            if let Some(metas) = metas {
                all_metas.extend(metas);
            }
        }

        Ok(all_metas)
    }
}

/// 将 proto ObjectMeta 转换为内部 ObjectMeta
fn proto_to_object_meta(meta_proto: proto::ObjectMeta) -> Result<ObjectMeta> {
    Ok(ObjectMeta {
        key: meta_proto.key,
        size: meta_proto.size,
        created_at: chrono::DateTime::parse_from_rfc3339(&meta_proto.created_at)
            .map_err(|e| StoreError::InvalidArgument(format!("时间解析失败: {}", e)))?
            .with_timezone(&Utc),
        updated_at: chrono::DateTime::parse_from_rfc3339(&meta_proto.updated_at)
            .map_err(|e| StoreError::InvalidArgument(format!("时间解析失败: {}", e)))?
            .with_timezone(&Utc),
        content_type: if meta_proto.content_type.is_empty() {
            None
        } else {
            Some(meta_proto.content_type)
        },
        tags: if meta_proto.tags.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(&meta_proto.tags)
                    .map_err(|e| StoreError::InvalidArgument(format!("Tags 解析失败: {}", e)))?,
            )
        },
        checksum: None,
        storage_node: None,
    })
}

// ============================================================
// Master gRPC 服务实现
// ============================================================

/// 对外 StoreService 实现（客户端 -> Master）
#[derive(Debug, Clone)]
pub struct MasterStoreService {
    master: Arc<MasterNode>,
}

impl MasterStoreService {
    pub fn new(master: MasterNode) -> Self {
        Self {
            master: Arc::new(master),
        }
    }

    pub fn new_arc(master: Arc<MasterNode>) -> Self {
        Self { master }
    }
}

#[tonic::async_trait]
impl proto::store_service_server::StoreService for MasterStoreService {
    async fn put(
        &self,
        request: Request<proto::PutRequest>,
    ) -> std::result::Result<Response<proto::PutResponse>, Status> {
        let req = request.into_inner();
        let value = Bytes::from(req.value);

        let content_type = if req.content_type.is_empty() {
            None
        } else {
            Some(req.content_type)
        };
        let tags = if req.tags.is_empty() {
            None
        } else {
            Some(
                serde_json::from_str(&req.tags)
                    .map_err(|e| Status::invalid_argument(format!("Invalid tags: {}", e)))?,
            )
        };

        let meta = self
            .master
            .put(req.key, value, content_type, tags)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::PutResponse {
            meta: Some(proto::ObjectMeta {
                key: meta.key,
                size: meta.size,
                created_at: meta.created_at.to_rfc3339(),
                updated_at: meta.updated_at.to_rfc3339(),
                content_type: meta.content_type.unwrap_or_default(),
                tags: meta.tags.map(|t| t.to_string()).unwrap_or_default(),
            }),
        }))
    }

    async fn get(
        &self,
        request: Request<proto::GetRequest>,
    ) -> std::result::Result<Response<proto::GetResponse>, Status> {
        let req = request.into_inner();

        let (value, meta) = self.master.get(&req.key).await.map_err(|e| match e {
            StoreError::KeyNotFound(_) => Status::not_found(format!("Key not found: {}", req.key)),
            _ => Status::internal(e.to_string()),
        })?;

        Ok(Response::new(proto::GetResponse {
            value: value.to_vec(),
            meta: Some(proto::ObjectMeta {
                key: meta.key,
                size: meta.size,
                created_at: meta.created_at.to_rfc3339(),
                updated_at: meta.updated_at.to_rfc3339(),
                content_type: meta.content_type.unwrap_or_default(),
                tags: meta.tags.map(|t| t.to_string()).unwrap_or_default(),
            }),
        }))
    }

    async fn delete(
        &self,
        request: Request<proto::DeleteRequest>,
    ) -> std::result::Result<Response<proto::DeleteResponse>, Status> {
        let req = request.into_inner();

        self.master
            .delete(&req.key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::DeleteResponse { success: true }))
    }

    async fn exists(
        &self,
        request: Request<proto::ExistsRequest>,
    ) -> std::result::Result<Response<proto::ExistsResponse>, Status> {
        let req = request.into_inner();

        let exists = self
            .master
            .exists(&req.key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::ExistsResponse { exists }))
    }

    async fn list(
        &self,
        request: Request<proto::ListRequest>,
    ) -> std::result::Result<Response<proto::ListResponse>, Status> {
        let req = request.into_inner();

        let metas = self
            .master
            .list(&req.prefix, req.limit as usize)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_metas = metas
            .into_iter()
            .map(|m| proto::ObjectMeta {
                key: m.key,
                size: m.size,
                created_at: m.created_at.to_rfc3339(),
                updated_at: m.updated_at.to_rfc3339(),
                content_type: m.content_type.unwrap_or_default(),
                tags: m.tags.map(|t| t.to_string()).unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(proto::ListResponse { metas: proto_metas }))
    }

    async fn put_batch(
        &self,
        request: Request<proto::PutBatchRequest>,
    ) -> std::result::Result<Response<proto::PutBatchResponse>, Status> {
        let req = request.into_inner();

        let mut items = Vec::with_capacity(req.items.len());
        for item in req.items {
            let content_type = if item.content_type.is_empty() {
                None
            } else {
                Some(item.content_type)
            };
            let tags = if item.tags.is_empty() {
                None
            } else {
                Some(
                    serde_json::from_str(&item.tags)
                        .map_err(|e| Status::invalid_argument(format!("Invalid tags: {}", e)))?,
                )
            };

            items.push((item.key, Bytes::from(item.value), content_type, tags));
        }

        let metas = self
            .master
            .put_batch(items)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_metas = metas
            .into_iter()
            .map(|m| proto::ObjectMeta {
                key: m.key,
                size: m.size,
                created_at: m.created_at.to_rfc3339(),
                updated_at: m.updated_at.to_rfc3339(),
                content_type: m.content_type.unwrap_or_default(),
                tags: m.tags.map(|t| t.to_string()).unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(proto::PutBatchResponse {
            metas: proto_metas,
        }))
    }
}

/// Master 管理服务实现（Worker 注册/心跳）
#[derive(Debug, Clone)]
pub struct MasterAdminService {
    master: Arc<MasterNode>,
}

impl MasterAdminService {
    pub fn new(master: MasterNode) -> Self {
        Self {
            master: Arc::new(master),
        }
    }

    pub fn new_arc(master: Arc<MasterNode>) -> Self {
        Self { master }
    }
}

#[tonic::async_trait]
impl proto::master_service_server::MasterService for MasterAdminService {
    async fn register_worker(
        &self,
        request: Request<proto::RegisterWorkerRequest>,
    ) -> std::result::Result<Response<proto::RegisterWorkerResponse>, Status> {
        let req = request.into_inner();

        self.master
            .register_worker(&req.worker_id, &req.address, req.weight, req.tags)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::RegisterWorkerResponse {
            success: true,
            message: format!("Worker {} 注册成功", req.worker_id),
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<proto::HeartbeatRequest>,
    ) -> std::result::Result<Response<proto::HeartbeatResponse>, Status> {
        let req = request.into_inner();

        let payload = HeartbeatPayload {
            storage_used_bytes: req.storage_used_bytes,
            storage_capacity_bytes: req.storage_capacity_bytes,
            active_connections: req.active_connections,
            storage_usage_ratio: req.storage_usage_ratio,
            disk_health: req.disk_health,
            memory_used_bytes: req.memory_used_bytes,
            memory_total_bytes: req.memory_total_bytes,
            memory_usage_ratio: req.memory_usage_ratio,
            cpu_usage_ratio: req.cpu_usage_ratio,
            cpu_cores: req.cpu_cores,
            total_put_count: req.total_put_count,
            total_put_bytes: req.total_put_bytes,
            flushed_count: req.flushed_count,
            flushed_bytes: req.flushed_bytes,
            pending_count: req.pending_count,
            pending_bytes: req.pending_bytes,
            write_rate_per_sec: req.write_rate_per_sec,
            write_bytes_per_sec: req.write_bytes_per_sec,
        };
        let success = self
            .master
            .heartbeat(&req.worker_id, payload)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::HeartbeatResponse {
            success,
            message: if success {
                "OK".to_string()
            } else {
                "Worker not found".to_string()
            },
        }))
    }

    async fn list_workers(
        &self,
        request: Request<proto::ListWorkersRequest>,
    ) -> std::result::Result<Response<proto::ListWorkersResponse>, Status> {
        let req = request.into_inner();

        let workers = self.master.list_workers(req.only_alive).await;

        let proto_workers = workers
            .into_iter()
            .map(|w| proto::WorkerInfo {
                worker_id: w.worker_id,
                address: w.address,
                weight: w.weight,
                alive: w.alive,
                last_heartbeat: w.last_heartbeat,
                storage_used_bytes: w.storage_used_bytes,
                storage_capacity_bytes: w.storage_capacity_bytes,
                active_connections: w.active_connections,
                tags: w.tags,
                storage_usage_ratio: w.storage_usage_ratio,
                disk_health: w.disk_health,
                memory_used_bytes: w.memory_used_bytes,
                memory_total_bytes: w.memory_total_bytes,
                memory_usage_ratio: w.memory_usage_ratio,
                cpu_usage_ratio: w.cpu_usage_ratio,
                cpu_cores: w.cpu_cores,
                total_put_count: w.total_put_count,
                total_put_bytes: w.total_put_bytes,
                flushed_count: w.flushed_count,
                flushed_bytes: w.flushed_bytes,
                pending_count: w.pending_count,
                pending_bytes: w.pending_bytes,
                write_rate_per_sec: w.write_rate_per_sec,
                write_bytes_per_sec: w.write_bytes_per_sec,
            })
            .collect();

        Ok(Response::new(proto::ListWorkersResponse {
            workers: proto_workers,
        }))
    }

    async fn get_route(
        &self,
        request: Request<proto::GetRouteRequest>,
    ) -> std::result::Result<Response<proto::GetRouteResponse>, Status> {
        let req = request.into_inner();

        let worker = self
            .master
            .route(&req.key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::GetRouteResponse {
            worker_id: worker.worker_id,
            address: worker.address,
        }))
    }
}
