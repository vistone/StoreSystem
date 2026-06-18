use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use store_system::{
    AppConfig, MasterNode, MasterStoreService, MasterAdminService,
    WorkerNode, WorkerConfig, WorkerService,
    grpc, http, Store,
    logger::{LogStore, WorkerLogger, LogLevel, LogCategory},
    master_admin_http::{AdminContext, start_admin_api},
    master_log_ws::MasterLogWsServer,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    // 解析命令行参数
    let mut config_path = "config.yaml".to_string();
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

    let config = AppConfig::from_file(&config_path);

    // 确定运行模式：命令行 > 配置文件
    let mode = mode.unwrap_or_else(|| config.mode.clone());


    match mode.as_str() {
        "master" => run_master(&config).await,
        "worker" => run_worker(&config).await,
        "standalone" => run_standalone(&config).await,
        _ => {
            eprintln!("用法: {} [--config <path>] [master|worker|standalone]", args[0]);
            eprintln!();
            eprintln!("  方式一（推荐）：使用专用配置文件");
            eprintln!("    Master:   ./store_system --config master.yaml");
            eprintln!("    Worker:   ./store_system --config worker.yaml");
            eprintln!("    单机:     ./store_system --config config.yaml standalone");
            eprintln!();
            eprintln!("  方式二：使用通用配置文件 + 命令行模式");
            eprintln!("    Master:   ./store_system --config config.yaml master");
            eprintln!("    Worker:   ./store_system --config config.yaml worker");
            eprintln!("    单机:     ./store_system --config config.yaml standalone");
            eprintln!();
            eprintln!("  方式三：使用默认 config.yaml（mode 字段决定模式）");
            eprintln!("    ./store_system");
            Ok(())
        }
    }
}

/// 启动 Master 节点
async fn run_master(config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 启动 Master 节点 ===");

    let mc = &config.master;
    let gc = &config.global;

    let master = MasterNode::open(
        store_system::master::MasterConfig {
            listen_addr: mc.listen_addr.clone(),
            meta_path: mc.meta_path.clone(),
            heartbeat_timeout_secs: mc.heartbeat_timeout_secs,
            cleanup_interval_secs: mc.cleanup_interval_secs,
        }
    )?;
    let master = Arc::new(master);

    // 初始化日志存储（SQLite 持久化）
    let log_store_path = format!("{}_logs.db", mc.meta_path.trim_end_matches(".db"));
    let log_store = LogStore::open(&log_store_path)?;
    let log_store = Arc::new(log_store);
    println!("📋 日志数据库: {}", log_store_path);

    // 启动后台清理任务
    let cleanup_master = master.clone();
    let cleanup_interval = Duration::from_secs(mc.cleanup_interval_secs);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(cleanup_interval).await;
            cleanup_master.cleanup_dead_workers().await;
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
                store_system::grpc::proto::store_service_server::StoreServiceServer::new(store_service)
                    .max_decoding_message_size(max_msg)
                    .max_encoding_message_size(max_msg)
            )
            .add_service(
                store_system::grpc::proto::master_service_server::MasterServiceServer::new(admin_service)
                    .max_decoding_message_size(max_msg)
                    .max_encoding_message_size(max_msg)
            )
            .serve(addr)
            .await
            .expect("gRPC server failed");
    };

    // Admin API 服务（为前端提供 RESTful 接口）
    let admin_ctx = AdminContext::new(master.clone(), log_store.as_ref().clone());
    let admin_port = 50052;
    let admin_server = async {
        start_admin_api(admin_ctx, admin_port).await;
    };

    // 日志 WebSocket 服务（接收 Worker 推送的日志）
    let log_ws_port = 50053;
    let log_ws_server = MasterLogWsServer::new(log_store.as_ref().clone(), log_ws_port);
    let log_ws_server = async {
        log_ws_server.start().await;
    };

    println!("   📊 Admin API: http://0.0.0.0:{}", admin_port);
    println!("   📋 Log WS: ws://0.0.0.0:{}", log_ws_port);

    tokio::join!(grpc_server, admin_server, log_ws_server);
    Ok(())
}

/// 启动 Worker 节点
async fn run_worker(config: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    println!("=== 启动 Worker 节点 ===");

    let wc = &config.worker;

    // 确保数据目录存在
    std::fs::create_dir_all(&wc.data_dir)?;

    let worker_config = WorkerConfig::new(
        &wc.worker_id,
        &wc.listen_addr,
        &wc.master_addr,
        &wc.data_dir,
    )
    .with_kv_path(wc.kv_path().to_string_lossy())
    .with_meta_path(wc.meta_path().to_string_lossy())
    .with_cache_size(wc.cache_size)
    .with_flush_interval(wc.flush_interval_ms)
    .with_heartbeat_interval(wc.heartbeat_interval_secs)
    .with_weight(wc.weight);

    let node = WorkerNode::open(worker_config)?;

    // 使用 Arc 共享 WorkerNode 实例，避免重复打开数据库文件
    let node_arc = Arc::new(node);
    let worker_service = WorkerService::new_arc(node_arc.clone());

    // 根据协议启动对应的服务
    let protocol = &config.global.protocol;
    let ws_port = wc.listen_addr.rsplit(':').next().unwrap_or("50061").parse::<u16>().unwrap_or(50061) + 1000;
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

    // 注册到 Master
    let master_addr_http = if wc.master_addr.starts_with("http") {
        wc.master_addr.clone()
    } else {
        format!("http://{}", wc.master_addr)
    };

    println!("   注册到 Master: {}/{}", master_addr_http, wc.worker_id);
    match register_with_master(&master_addr_http, &wc.worker_id, &wc.listen_addr).await {
        Ok(_) => println!("   ✅ 注册成功"),
        Err(e) => eprintln!("   ⚠️  注册失败: {} (Master 可能未启动)", e),
    }

    // 启动心跳
    let hb_master_addr = master_addr_http.clone();
    let hb_worker_id = wc.worker_id.clone();
    let hb_interval = wc.heartbeat_interval_secs;
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
    let worker_logger = std::sync::Arc::new(WorkerLogger::new(
        &wc.worker_id,
        &wc.master_ws_addr,
    )
    .with_flush_interval(1000)
    .with_max_buffer(500));

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
    worker_logger.info(LogCategory::SystemHealth, &format!(
        "Worker '{}' 启动成功, KV: {}, Meta: {}",
        wc.worker_id,
        wc.kv_path().display(),
        wc.meta_path().display()
    ));

    let max_msg = config.global.max_message_size;
    let addr: std::net::SocketAddr = wc.listen_addr.parse()?;

    println!("🚀 Worker 节点 '{}' 启动在 http://{}", wc.worker_id, addr);
    println!("   KV 数据库: {}", wc.kv_path().display());
    println!("   Meta 数据库: {}", wc.meta_path().display());
    println!("   心跳间隔: {}s", hb_interval);
    println!("   日志推送: ws://{}", wc.master_ws_addr);

    Server::builder()
        .add_service(
            store_system::grpc::proto::worker_service_server::WorkerServiceServer::new(worker_service)
                .max_decoding_message_size(max_msg)
                .max_encoding_message_size(max_msg)
        )
        .serve(addr)
        .await?;

    Ok(())
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
    store.start_flusher(sc.flush_interval_ms, 1000);
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
        let grpc_service = store_system::grpc::proto::store_service_server::StoreServiceServer::new(
            grpc::GrpcStoreService::new(grpc_store)
        )
            .max_decoding_message_size(max_msg)
            .max_encoding_message_size(max_msg);
        let addr = ([0, 0, 0, 0], grpc_port).into();
        println!("🚀 gRPC server running on http://0.0.0.0:{} (max msg: {}MB)", grpc_port, max_msg / (1024 * 1024));
        Server::builder()
            .add_service(grpc_service)
            .serve(addr)
            .await
            .expect("gRPC server failed");
    };

    println!("\n🎉 单机模式启动成功！");
    println!("🌐 RESTful API 地址: http://0.0.0.0:{}/objects", sc.http_port);
    println!("🚀 gRPC 服务地址: 0.0.0.0:{}", sc.grpc_port);

    tokio::join!(http_server, grpc_server);
    Ok(())
}

/// 注册 Worker 到 Master
async fn register_with_master(master_addr: &str, worker_id: &str, listen_addr: &str) -> Result<(), Box<dyn std::error::Error>> {
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
    });

    client.register_worker(request).await?;
    Ok(())
}

/// 发送心跳到 Master（携带系统健康信息）
async fn send_heartbeat(
    master_addr: &str,
    worker_id: &str,
    node: &std::sync::Arc<store_system::worker::WorkerNode>,
) -> Result<(), Box<dyn std::error::Error>> {
    use store_system::grpc::proto::master_service_client::MasterServiceClient;
    use store_system::health::HealthInfo;

    // 采集系统健康信息
    let health = HealthInfo::collect(".");

    // 采集 Worker 写入统计快照
    let write_stats = node.write_stats_snapshot();

    let endpoint = tonic::transport::Endpoint::from_shared(master_addr.to_string())?
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(5));

    let mut client = MasterServiceClient::connect(endpoint).await?;

    let request = tonic::Request::new(store_system::grpc::proto::HeartbeatRequest {
        worker_id: worker_id.to_string(),
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
    });

    client.heartbeat(request).await?;
    Ok(())
}

