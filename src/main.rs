use std::sync::Arc;
use std::time::Duration;
use store_system::{
    grpc, http,
    logger::{LogCategory, LogStore, WorkerLogger},
    master_admin_http::{start_admin_api, AdminContext},
    master_log_ws::MasterLogWsServer,
    AppConfig, MasterAdminService, MasterNode, MasterStoreService, Store, WorkerConfig, WorkerNode,
    WorkerService,
};
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    // 解析命令行参数
    // 默认配置文件为 master.yaml（Master 统一配置管理架构）
    let mut config_path = "master.yaml".to_string();
    let mut mode: Option<String> = None;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--config" {
            if i + 1 < args.len() {
                config_path = args[i + 1].clone();
                i += 2;
                continue;
            }
        } else if !args[i].starts_with("--") {
            mode = Some(args[i].clone());
        }
        i += 1;
    }

    let config = AppConfig::from_file(&config_path).unwrap_or_else(|e| {
        eprintln!("[Config] 配置加载失败: {}，使用默认配置", e);
        AppConfig::default()
    });

    // 确定运行模式：命令行 > 配置文件
    let mode = mode.unwrap_or_else(|| config.mode.clone());

    match mode.as_str() {
        "master" => run_master(&config).await,
        "worker" => run_worker(&config).await,
        "standalone" => run_standalone(&config).await,
        _ => {
            eprintln!(
                "用法: {} [--config <path>] [master|worker|standalone]",
                args[0]
            );
            eprintln!();
            eprintln!("  Master 统一配置管理架构：");
            eprintln!("    Master:   ./store_system --config master.yaml");
            eprintln!("    Worker:   ./store_system --config worker-0.yaml");
            eprintln!("    单机:     ./store_system --config master.yaml standalone");
            eprintln!();
            eprintln!("  不指定 --config 时默认加载 master.yaml");
            Ok(())
        }
    }
}

/// 启动 Master 节点
async fn run_master(config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 启动 Master 节点 ===");

    let mc = &config.master;
    let gc = &config.global;

    let mut master = MasterNode::open_with_worker_defaults(
        store_system::master::MasterConfig {
            listen_addr: mc.listen_addr.clone(),
            meta_path: mc.meta_path.clone(),
            heartbeat_timeout_secs: mc.heartbeat_timeout_secs,
            cleanup_interval_secs: mc.cleanup_interval_secs,
            pending_data_dir: mc.pending.data_dir.clone(),
            pending_gc_interval_secs: mc.pending.gc_interval_secs,
            pending_flush_timeout_secs: mc.pending.flush_timeout_secs,
        },
        &gc.protocol,
        config.worker_defaults.clone(),
        config.worker_regions.clone(),
    )?;

    // 初始化日志存储（SQLite 持久化）
    let log_store_path = format!("{}_logs.db", mc.meta_path.trim_end_matches(".db"));
    let log_store = LogStore::open(&log_store_path)?;
    let log_store = Arc::new(log_store);
    println!("📋 日志数据库: {}", log_store_path);

    // 初始化日志 WS 服务（先创建以获取 broadcaster）
    let log_ws_port = 50053;
    let pending_store = master.pending_store.clone();
    let log_ws_server = MasterLogWsServer::new(
        log_store.as_ref().clone(),
        log_ws_port,
        pending_store.clone(),
    );
    let broadcaster = log_ws_server.config_broadcaster();

    // 设置 Master 的配置推送器（用于配置热更新推送）
    master.set_config_broadcaster(broadcaster);

    let master = Arc::new(master);

    // 启动后台清理任务（宕机 Worker 检测）
    let cleanup_master = master.clone();
    let cleanup_interval = Duration::from_secs(mc.cleanup_interval_secs);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(cleanup_interval).await;
            cleanup_master.cleanup_dead_workers().await;
        }
    });

    // 启动后台 Pending GC 任务
    let pending_master = master.clone();
    let pending_gc_interval = Duration::from_secs(mc.pending.gc_interval_secs);
    let pending_flush_timeout = mc.pending.flush_timeout_secs;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(pending_gc_interval).await;
            for region in &["0", "1", "2", "3"] {
                let _ = pending_master.pending_store.revert_stale_flushing(region, pending_flush_timeout);
                let _ = pending_master.pending_store.gc_done(region, pending_flush_timeout);
            }
        }
    });

    // 构建 gRPC 服务
    let store_service = MasterStoreService::new_arc(master.clone());
    let admin_service = MasterAdminService::new_arc(master.clone());

    let max_msg = gc.max_message_size;
    let addr: std::net::SocketAddr = mc.listen_addr.parse()?;

    println!("🚀 Master 节点启动在 http://{}", addr);
    println!("   对外服务: StoreService (Put/Get/Delete/Exists/List/PutBatch)");
    println!("   管理接口: MasterService (RegisterWorker/Heartbeat/ListWorkers/GetRoute)");
    println!("   心跳超时: {}s", mc.heartbeat_timeout_secs);
    println!("   清理间隔: {}s", mc.cleanup_interval_secs);
    println!("   最大消息: {}MB", max_msg / (1024 * 1024));

    // gRPC 主服务
    let grpc_server = async {
        Server::builder()
            .add_service(
                store_system::grpc::proto::store_service_server::StoreServiceServer::new(
                    store_service,
                )
                .max_decoding_message_size(max_msg)
                .max_encoding_message_size(max_msg),
            )
            .add_service(
                store_system::grpc::proto::master_service_server::MasterServiceServer::new(
                    admin_service,
                )
                .max_decoding_message_size(max_msg)
                .max_encoding_message_size(max_msg),
            )
            .serve(addr)
            .await
            .map_err(|e| format!("gRPC 服务器错误: {}", e))
    };

    // Admin API 服务（为前端提供 RESTful 接口）
    let admin_ctx = AdminContext::new(
        master.clone(),
        log_store.as_ref().clone(),
        master.pending_store.clone(),
    );
    let admin_port = 50052;
    let admin_server = async {
        start_admin_api(admin_ctx, admin_port).await;
    };

    // 日志 WebSocket 服务（接收 Worker 推送的日志 + pending 协议）
    let log_ws_port = 50053;
    let log_ws_server = MasterLogWsServer::new(
        log_store.as_ref().clone(),
        log_ws_port,
        master.pending_store.clone(),
    );
    let log_ws_server = async {
        log_ws_server.start().await;
    };

    println!("   📊 Admin API: http://0.0.0.0:{}", admin_port);
    println!("   📋 Log WS: ws://0.0.0.0:{}", log_ws_port);

    let (grpc_result, _, _) = tokio::join!(grpc_server, admin_server, log_ws_server);
    grpc_result.map_err(|e| e.into())
}

/// 启动 Worker 节点
async fn run_worker(config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 启动 Worker 节点 ===");

    let wc = &config.worker;

    // 确保数据目录存在
    std::fs::create_dir_all(&wc.data_dir)?;

    // 推导 Master WebSocket 地址（同主机 + :50053）
    let master_ws_addr = derive_master_ws_addr(&wc.master_addr);

    // 注册到 Master（带重试，应对 Master 启动延迟）
    let master_addr_http = if wc.master_addr.starts_with("http") {
        wc.master_addr.clone()
    } else {
        format!("http://{}", wc.master_addr)
    };

    println!("   注册到 Master: {}/{}", master_addr_http, wc.worker_id);

    let proto_config = {
        let mut acquired: Option<store_system::grpc::proto::WorkerConfig> = None;
        for retry in 0..10 {
            match register_with_master(&master_addr_http, &wc.worker_id, &wc.listen_addr).await {
                Ok(cfg) => {
                    println!("   ✅ 注册成功, 配置版本: 来自 Master");
                    acquired = Some(cfg);
                    break;
                }
                Err(e) => {
                    if retry < 9 {
                        let delay = std::time::Duration::from_millis(1000 * (retry + 1) as u64);
                        eprintln!("   ⚠️  注册失败 (重试 {}/10): {}", retry + 1, e);
                        tokio::time::sleep(delay).await;
                    } else {
                        return Err(format!("❌ 注册最终失败: {}", e).into());
                    }
                }
            }
        }
        acquired.expect("注册成功后必有配置")
    };

    // 用 Master 下发的配置构建 WorkerConfig
    let quad_shard_config = proto_config
        .quad_shard
        .as_ref()
        .map(|qs| store_system::config::QuadShardConfig {
            base_level: qs.base_level,
            split_level: qs.split_level,
            data_dir: qs.data_dir.clone(),
            kv_ext: qs.kv_ext.clone(),
            meta_ext: qs.meta_ext.clone(),
            cache_size: qs.cache_size as usize,
            flush_interval_ms: qs.flush_interval_ms,
        });

    let kv_path = format!("{}/kv{}", wc.data_dir, proto_config.kv_ext);
    let meta_path = format!("{}/meta{}", wc.data_dir, proto_config.meta_ext);

    let worker_config = WorkerConfig::new(
        &wc.worker_id,
        &wc.listen_addr,
        &wc.master_addr,
        &wc.data_dir,
    )
    .with_kv_path(&kv_path)
    .with_meta_path(&meta_path)
    .with_cache_size(proto_config.cache_size as usize)
    .with_flush_interval(proto_config.flush_interval_ms)
    .with_heartbeat_interval(proto_config.heartbeat_interval_secs)
    .with_weight(proto_config.weight)
    .with_quad_shard_config_option(quad_shard_config);

    println!("   QuadKey 区域: {}", proto_config.region);
    println!("   KV 扩展名: {}", proto_config.kv_ext);
    println!("   Meta 扩展名: {}", proto_config.meta_ext);

    let node = WorkerNode::open(worker_config)?;

    // 使用 Arc 共享 WorkerNode 实例，避免重复打开数据库文件
    let node_arc = Arc::new(node);

    // === 从 Master 拉取 pending 数据（region 恢复后写回） ===
    let pending_region = proto_config.region.clone();
    let pending_node = node_arc.clone();
    let pending_ws = master_ws_addr.clone();
    let pending_count = tokio::spawn(async move {
        match pull_pending_from_master(&pending_ws, &pending_region, &pending_node).await {
            Ok(n) => {
                if n > 0 {
                    println!("   📥 从 Master 拉取了 {} 条 pending 数据", n);
                }
                n
            }
            Err(e) => {
                eprintln!("   ⚠️  拉取 pending 数据失败: {}", e);
                0
            }
        }
    })
    .await
    .unwrap_or(0);
    let _ = pending_count;

    let worker_service = WorkerService::new_arc(node_arc.clone());

    // 根据协议启动对应的服务
    let protocol = &config.global.protocol;
    let ws_port = wc
        .listen_addr
        .rsplit(':')
        .next()
        .unwrap_or("50061")
        .parse::<u16>()
        .unwrap_or(50061)
        + 1000;
    let http_port = ws_port + 1000;

    // 如果协议是 ws 或 both，启动 WebSocket 服务
    if protocol == "ws" || protocol == "both" {
        let ws_node = node_arc.clone();
        tokio::spawn(async move {
            store_system::worker_ws::start_worker_ws_server(ws_node, ws_port).await;
        });
        println!("   🔌 WebSocket 服务: ws://0.0.0.0:{}", ws_port);
    }

    // 如果协议是 restful 或 both，启动 HTTP 服务
    if protocol == "restful" || protocol == "both" {
        let http_node = node_arc.clone();
        tokio::spawn(async move {
            store_system::worker_http::start_worker_http_server(http_node, http_port).await;
        });
        println!("   🌐 RESTful API: http://0.0.0.0:{}", http_port);
    }

    // 启动心跳
    let hb_master_addr = master_addr_http.clone();
    let hb_worker_id = wc.worker_id.clone();
    let hb_interval = proto_config.heartbeat_interval_secs;
    let hb_node = node_arc.clone();
    // 启动单库模式的后台刷盘任务（分片模式由 ShardManager 内部启动）
    hb_node.start_flusher();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(hb_interval)).await;
            if let Err(e) = send_heartbeat(&hb_master_addr, &hb_worker_id, &hb_node).await {
                eprintln!("[Worker] 心跳失败: {}", e);
            }
        }
    });

    // 初始化 Worker 日志采集器（使用持久 WebSocket 连接）
    // 同时承载配置更新回调：Master 推送 config_update 时触发 WorkerNode 热更新
    let logger_node = node_arc.clone();
    let worker_logger = std::sync::Arc::new(
        WorkerLogger::new(&wc.worker_id, &master_ws_addr)
            .with_flush_interval(1000)
            .with_max_buffer(500)
            .with_config_update_handler(move |config_json| {
                // 解析配置 JSON 并调用 WorkerNode 热更新接口
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&config_json) {
                    let cache_size = v
                        .get("cache_size")
                        .and_then(|n| n.as_u64())
                        .map(|n| n as usize);
                    let flush_interval_ms = v
                        .get("flush_interval_ms")
                        .and_then(|n| n.as_u64());
                    let heartbeat_interval_secs = v
                        .get("heartbeat_interval_secs")
                        .and_then(|n| n.as_u64());
                    let weight = v.get("weight").and_then(|n| n.as_i64()).map(|n| n as i32);
                    logger_node.update_performance_config(
                        cache_size,
                        flush_interval_ms,
                        heartbeat_interval_secs,
                        weight,
                    );
                }
            }),
    );

    // 启动后台持久 WebSocket 连接
    let bg_logger = worker_logger.clone();
    tokio::spawn(async move {
        bg_logger.start_background_connection().await;
    });

    // 等待连接建立
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 启动定时日志推送
    let logger_for_flush = worker_logger.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(1000)).await;
            let _ = logger_for_flush.flush().await;
        }
    });

    // 记录启动日志
    worker_logger.info(
        LogCategory::SystemHealth,
        &format!(
            "Worker '{}' 启动成功, KV: {}, Meta: {}",
            wc.worker_id, kv_path, meta_path
        ),
    );

    let max_msg = config.global.max_message_size;
    let addr: std::net::SocketAddr = wc.listen_addr.parse()?;

    println!("🚀 Worker 节点 '{}' 启动在 http://{}", wc.worker_id, addr);
    println!("   KV 数据库: {}", kv_path);
    println!("   Meta 数据库: {}", meta_path);
    println!("   心跳间隔: {}s", hb_interval);
    println!("   日志推送: ws://{}", master_ws_addr);

    Server::builder()
        .add_service(
            store_system::grpc::proto::worker_service_server::WorkerServiceServer::new(
                worker_service,
            )
            .max_decoding_message_size(max_msg)
            .max_encoding_message_size(max_msg),
        )
        .serve(addr)
        .await?;

    Ok(())
}

/// 从 Master 拉取本 region 的 pending 数据并写入本地
///
/// 在 Worker 启动注册成功后调用，通过 WebSocket 接收 pending_entry 流。
async fn pull_pending_from_master(
    master_ws_addr: &str,
    region: &str,
    node: &Arc<WorkerNode>,
) -> Result<usize, Box<dyn std::error::Error>> {
    use base64::Engine;
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;

    let ws_url = format!("ws://{}", master_ws_addr);
    let (ws_stream, _) = connect_async(&ws_url).await?;
    let (mut write, mut read) = ws_stream.split();

    // 发送 pending_pull 请求
    let pull_msg = serde_json::json!({
        "type": "pending_pull",
        "region": region,
    });
    write
        .send(tokio_tungstenite::tungstenite::Message::Text(
            pull_msg.to_string(),
        ))
        .await?;

    let mut count = 0usize;

    // 接收 pending_entry 流
    while let Some(msg) = read.next().await {
        let msg = msg?;
        if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
            let v: serde_json::Value = match serde_json::from_str(&text) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let msg_type = v.get("type").and_then(|t| t.as_str()).unwrap_or("");

            match msg_type {
                "pending_entry" => {
                    let key = v.get("key").and_then(|k| k.as_str()).unwrap_or("");
                    let b64 = v.get("value").and_then(|v| v.as_str()).unwrap_or("");
                    let value = base64::engine::general_purpose::STANDARD.decode(b64)?;

                    // 写入 Worker 本地存储（使用 put_object，走 WAL 三步协议）
                    let now = chrono::Utc::now();
                    let meta = store_system::ObjectMeta {
                        key: key.to_string(),
                        size: value.len() as u64,
                        created_at: now,
                        updated_at: now,
                        content_type: None,
                        tags: None,
                        checksum: None,
                        storage_node: None,
                    };
                    match node.put_object(key, bytes::Bytes::from(value), meta) {
                        Ok(()) => {
                            // 发送 ack
                            let ack = serde_json::json!({
                                "type": "pending_ack",
                                "key": key,
                                "region": region,
                                "status": "ok",
                            });
                            let _ = write
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    ack.to_string(),
                                ))
                                .await;
                            count += 1;
                        }
                        Err(e) => {
                            let ack = serde_json::json!({
                                "type": "pending_ack",
                                "key": key,
                                "region": region,
                                "status": "fail",
                                "error": e.to_string(),
                            });
                            let _ = write
                                .send(tokio_tungstenite::tungstenite::Message::Text(
                                    ack.to_string(),
                                ))
                                .await;
                        }
                    }
                }
                "pending_end" => {
                    break;
                }
                _ => {}
            }
        }
    }

    Ok(count)
}

/// 启动单机模式（向后兼容）
async fn run_standalone(config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 启动单机模式（向后兼容）===");

    let sc = &config.standalone;

    // 确保数据目录存在
    if let Some(parent) = std::path::Path::new(&sc.kv_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let store = Store::open(&sc.kv_path, &sc.meta_path, sc.cache_size)?;
    store.start_flusher(sc.flush_interval_ms);
    println!("✅ Store (单库) 初始化完成！");

    // 启动 RESTful HTTP 服务
    let http_store = store.clone();
    let http_port = sc.http_port;
    let http_server = async move {
        http::start_server(http_store, http_port).await;
    };

    // 启动 gRPC 服务
    let grpc_store = store.clone();
    let grpc_port = sc.grpc_port;
    let max_msg = config.global.max_message_size;
    let grpc_server = async move {
        let grpc_service =
            store_system::grpc::proto::store_service_server::StoreServiceServer::new(
                grpc::GrpcStoreService::new(grpc_store),
            )
            .max_decoding_message_size(max_msg)
            .max_encoding_message_size(max_msg);
        let addr = ([0, 0, 0, 0], grpc_port).into();
        println!(
            "🚀 gRPC server running on http://0.0.0.0:{} (max msg: {}MB)",
            grpc_port,
            max_msg / (1024 * 1024)
        );
        if let Err(e) = Server::builder()
            .add_service(grpc_service)
            .serve(addr)
            .await
        {
            eprintln!("gRPC 服务器退出: {}", e);
        }
    };

    println!("\n🎉 单机模式启动成功！");
    println!(
        "🌐 RESTful API 地址: http://0.0.0.0:{}/objects",
        sc.http_port
    );
    println!("🚀 gRPC 服务地址: 0.0.0.0:{}", sc.grpc_port);

    tokio::join!(http_server, grpc_server);
    Ok(())
}

/// 从 master_addr 推导 master_ws_addr（同主机 + :50053）
///
/// 例: "http://127.0.0.1:50051" -> "127.0.0.1:50053"
fn derive_master_ws_addr(master_addr: &str) -> String {
    let host = master_addr
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split(':')
        .next()
        .unwrap_or("127.0.0.1");
    format!("{}:50053", host)
}

/// 注册 Worker 到 Master
async fn register_with_master(
    master_addr: &str,
    worker_id: &str,
    listen_addr: &str,
) -> Result<store_system::grpc::proto::WorkerConfig, Box<dyn std::error::Error>> {
    use store_system::grpc::proto::master_service_client::MasterServiceClient;

    let endpoint = tonic::transport::Endpoint::from_shared(master_addr.to_string())?
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5));

    let mut client = MasterServiceClient::connect(endpoint).await?;

    let request = tonic::Request::new(store_system::grpc::proto::RegisterWorkerRequest {
        worker_id: worker_id.to_string(),
        address: listen_addr.to_string(),
        weight: 1,
        tags: std::collections::HashMap::new(),
        region: String::new(), // region 由 Master 根据 worker_regions 分配
    });

    let response = client.register_worker(request).await?.into_inner();
    if !response.success {
        return Err(format!("Master 拒绝注册: {}", response.message).into());
    }
    response
        .config
        .ok_or_else(|| "Master 未返回 WorkerConfig".into())
}

/// 发送心跳到 Master（携带系统健康信息）
async fn send_heartbeat(
    master_addr: &str,
    worker_id: &str,
    node: &std::sync::Arc<store_system::worker::WorkerNode>,
) -> Result<(), Box<dyn std::error::Error>> {
    use store_system::grpc::proto::master_service_client::MasterServiceClient;
    use store_system::health::HealthInfo;
    use store_system::HeartbeatPayload;

    // CPU 采样包含 100ms sleep，用 spawn_blocking 避免阻塞 tokio 线程
    let health = tokio::task::spawn_blocking(|| HealthInfo::collect("."))
        .await
        .unwrap_or_else(|_| HealthInfo::collect("."));

    // 采集 Worker 写入统计快照
    let write_stats = node.write_stats_snapshot();

    let payload = HeartbeatPayload {
        storage_used_bytes: health.storage_used_bytes,
        storage_capacity_bytes: health.storage_capacity_bytes,
        active_connections: 0,
        storage_usage_ratio: health.storage_usage_ratio,
        disk_health: format!("{:?}", health.disk_health),
        memory_used_bytes: health.memory_used_bytes,
        memory_total_bytes: health.memory_total_bytes,
        memory_usage_ratio: health.memory_usage_ratio,
        cpu_usage_ratio: health.cpu_usage_ratio,
        cpu_cores: health.cpu_cores,
        total_put_count: write_stats.total_put_count,
        total_put_bytes: write_stats.total_put_bytes,
        flushed_count: write_stats.flushed_count,
        flushed_bytes: write_stats.flushed_bytes,
        pending_count: write_stats.pending_count,
        pending_bytes: write_stats.pending_bytes,
        write_rate_per_sec: write_stats.write_rate_per_sec,
        write_bytes_per_sec: write_stats.write_bytes_per_sec,
    };

    let endpoint = tonic::transport::Endpoint::from_shared(master_addr.to_string())?
        .timeout(std::time::Duration::from_secs(10))
        .connect_timeout(std::time::Duration::from_secs(5));

    let mut client = MasterServiceClient::connect(endpoint).await?;

    let request = tonic::Request::new(store_system::grpc::proto::HeartbeatRequest {
        worker_id: worker_id.to_string(),
        storage_used_bytes: payload.storage_used_bytes,
        storage_capacity_bytes: payload.storage_capacity_bytes,
        active_connections: payload.active_connections,
        storage_usage_ratio: payload.storage_usage_ratio,
        disk_health: payload.disk_health,
        memory_used_bytes: payload.memory_used_bytes,
        memory_total_bytes: payload.memory_total_bytes,
        memory_usage_ratio: payload.memory_usage_ratio,
        cpu_usage_ratio: payload.cpu_usage_ratio,
        cpu_cores: payload.cpu_cores,
        total_put_count: payload.total_put_count,
        total_put_bytes: payload.total_put_bytes,
        flushed_count: payload.flushed_count,
        flushed_bytes: payload.flushed_bytes,
        pending_count: payload.pending_count,
        pending_bytes: payload.pending_bytes,
        write_rate_per_sec: payload.write_rate_per_sec,
        write_bytes_per_sec: payload.write_bytes_per_sec,
    });

    client.heartbeat(request).await?;
    Ok(())
}
