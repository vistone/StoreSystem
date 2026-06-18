use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tonic::{Request, Response, Status};
use crate::error::Result;

use crate::kv::KvStore;
use crate::meta::{MetaStore, ObjectMeta};
use crate::shard::{ShardManager, ShardConfig};
use crate::grpc::proto;

/// Worker 节点配置
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Worker 唯一标识
    pub worker_id: String,
    /// Worker 监听地址（gRPC 服务）
    pub listen_addr: String,
    /// Master 地址（用于注册和心跳）
    pub master_addr: String,
    /// KV 数据库路径（单库模式）
    pub kv_path: String,
    /// Meta 数据库路径（单库模式）
    pub meta_path: String,
    /// 缓存大小
    pub cache_size: usize,
    /// 刷盘间隔（毫秒）
    pub flush_interval_ms: u64,
    /// 心跳间隔（秒）
    pub heartbeat_interval_secs: u64,
    /// Worker 权重
    pub weight: i32,
    /// 分片配置（如果启用分片，则使用 ShardManager）
    pub shard_configs: Option<Vec<ShardConfig>>,
}

impl WorkerConfig {
    pub fn new(
        worker_id: impl Into<String>,
        listen_addr: impl Into<String>,
        master_addr: impl Into<String>,
        data_dir: impl AsRef<Path>,
    ) -> Self {
        let data_dir = data_dir.as_ref();
        Self {
            worker_id: worker_id.into(),
            listen_addr: listen_addr.into(),
            master_addr: master_addr.into(),
            kv_path: data_dir.join("kv.db").to_string_lossy().to_string(),
            meta_path: data_dir.join("meta.db").to_string_lossy().to_string(),
            cache_size: 10000,
            flush_interval_ms: 5,
            heartbeat_interval_secs: 10,
            weight: 1,
            shard_configs: None,
        }
    }

    pub fn with_kv_path(mut self, path: impl Into<String>) -> Self {
        self.kv_path = path.into();
        self
    }

    pub fn with_meta_path(mut self, path: impl Into<String>) -> Self {
        self.meta_path = path.into();
        self
    }

    pub fn with_cache_size(mut self, size: usize) -> Self {
        self.cache_size = size;
        self
    }

    pub fn with_flush_interval(mut self, ms: u64) -> Self {
        self.flush_interval_ms = ms;
        self
    }

    pub fn with_heartbeat_interval(mut self, secs: u64) -> Self {
        self.heartbeat_interval_secs = secs;
        self
    }

    pub fn with_weight(mut self, weight: i32) -> Self {
        self.weight = weight;
        self
    }

    /// 设置分片配置（启用分片模式）
    pub fn with_shards(mut self, configs: Vec<ShardConfig>) -> Self {
        self.shard_configs = Some(configs);
        self
    }
}

// ============================================================
// 写入统计 - 原子计数器，记录写入/刷盘指标
// ============================================================

/// Worker 写入统计快照（用于心跳上报）
#[derive(Debug, Clone, Default)]
pub struct WriteStatsSnapshot {
    pub total_put_count: u64,
    pub total_put_bytes: u64,
    pub flushed_count: u64,
    pub flushed_bytes: u64,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub write_rate_per_sec: f64,
    pub write_bytes_per_sec: f64,
}

/// 原子写入统计计数器
pub struct WriteStats {
    /// 累计写入操作数（收到 put 请求就 +1）
    total_put_count: AtomicU64,
    /// 累计写入字节数
    total_put_bytes: AtomicU64,
    /// 已刷盘操作数（落盘成功后 +1）
    flushed_count: AtomicU64,
    /// 已刷盘字节数
    flushed_bytes: AtomicU64,
    /// 近期写入速率采样窗口起点
    rate_window_start: std::sync::Mutex<Instant>,
    rate_window_start_count: AtomicU64,
    rate_window_start_bytes: AtomicU64,
}

impl WriteStats {
    fn new() -> Self {
        Self {
            total_put_count: AtomicU64::new(0),
            total_put_bytes: AtomicU64::new(0),
            flushed_count: AtomicU64::new(0),
            flushed_bytes: AtomicU64::new(0),
            rate_window_start: std::sync::Mutex::new(Instant::now()),
            rate_window_start_count: AtomicU64::new(0),
            rate_window_start_bytes: AtomicU64::new(0),
        }
    }

    fn record_put(&self, byte_len: u64) {
        self.total_put_count.fetch_add(1, Ordering::Relaxed);
        self.total_put_bytes.fetch_add(byte_len, Ordering::Relaxed);
    }

    fn record_flush(&self, count: u64, bytes: u64) {
        self.flushed_count.fetch_add(count, Ordering::Relaxed);
        self.flushed_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 生成快照，同时重置速率采样窗口
    fn snapshot(&self) -> WriteStatsSnapshot {
        let total_put_count = self.total_put_count.load(Ordering::Relaxed);
        let total_put_bytes = self.total_put_bytes.load(Ordering::Relaxed);
        let flushed_count = self.flushed_count.load(Ordering::Relaxed);
        let flushed_bytes = self.flushed_bytes.load(Ordering::Relaxed);

        let pending_count = total_put_count.saturating_sub(flushed_count);
        let pending_bytes = total_put_bytes.saturating_sub(flushed_bytes);

        // 计算近期写入速率（自上次快照以来的平均速率）
        let (write_rate_per_sec, write_bytes_per_sec) = {
            let mut start = self.rate_window_start.lock().unwrap();
            let elapsed = start.elapsed().as_secs_f64();
            let start_count = self.rate_window_start_count.load(Ordering::Relaxed);
            let start_bytes = self.rate_window_start_bytes.load(Ordering::Relaxed);

            let rate = if elapsed > 0.0 {
                (total_put_count.saturating_sub(start_count)) as f64 / elapsed
            } else {
                0.0
            };
            let byte_rate = if elapsed > 0.0 {
                (total_put_bytes.saturating_sub(start_bytes)) as f64 / elapsed
            } else {
                0.0
            };

            // 重置窗口
            *start = Instant::now();
            self.rate_window_start_count.store(total_put_count, Ordering::Relaxed);
            self.rate_window_start_bytes.store(total_put_bytes, Ordering::Relaxed);
            (rate, byte_rate)
        };

        WriteStatsSnapshot {
            total_put_count,
            total_put_bytes,
            flushed_count,
            flushed_bytes,
            pending_count,
            pending_bytes,
            write_rate_per_sec,
            write_bytes_per_sec,
        }
    }
}

impl std::fmt::Debug for WriteStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteStats")
            .field("total_put_count", &self.total_put_count.load(Ordering::Relaxed))
            .field("flushed_count", &self.flushed_count.load(Ordering::Relaxed))
            .finish()
    }
}

// ============================================================
// 写合并缓冲区 - put 先入内存，后台批量刷盘
// ============================================================

#[derive(Debug, Clone)]
enum PendingOp {
    Put { value: Bytes, meta: ObjectMeta },
    Delete,
}

struct WriteBuffer {
    pending: DashMap<String, PendingOp>,
    pending_count: AtomicU64,
    pending_bytes: AtomicU64,
    flush_notify: Notify,
    flushing: AtomicBool,
}

impl std::fmt::Debug for WriteBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBuffer")
            .field("pending_count", &self.pending_count.load(Ordering::Relaxed))
            .field("flushing", &self.flushing.load(Ordering::Relaxed))
            .finish()
    }
}

impl WriteBuffer {
    fn new() -> Self {
        Self {
            pending: DashMap::new(),
            pending_count: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            flush_notify: Notify::new(),
            flushing: AtomicBool::new(false),
        }
    }

    fn submit_put(&self, key: String, value: Bytes, meta: ObjectMeta, stats: &WriteStats) {
        let byte_len = value.len() as u64;
        self.pending.insert(key, PendingOp::Put { value, meta });
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        self.pending_bytes.fetch_add(byte_len, Ordering::Relaxed);
        stats.record_put(byte_len);
        // 通知 flusher 立即处理：空闲时立即落盘，不等待 interval 超时
        self.notify_flush();
    }

    /// 批量提交中的单条写入（不触发 notify，由调用方统一 notify）
    fn submit_put_silent(&self, key: String, value: Bytes, meta: ObjectMeta, stats: &WriteStats) {
        let byte_len = value.len() as u64;
        self.pending.insert(key, PendingOp::Put { value, meta });
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        self.pending_bytes.fetch_add(byte_len, Ordering::Relaxed);
        stats.record_put(byte_len);
    }

    fn submit_delete(&self, key: String, stats: &WriteStats) {
        self.pending.insert(key, PendingOp::Delete);
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        stats.record_put(0);
        // 通知 flusher 立即处理
        self.notify_flush();
    }

    fn drain(&self) -> HashMap<String, PendingOp> {
        // 取出当前所有 pending 数据
        let items: Vec<(String, PendingOp)> = self.pending.iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect();
        let drained_count = items.len() as u64;
        let drained_bytes: u64 = items.iter().map(|(_, op)| {
            match op {
                PendingOp::Put { value, .. } => value.len() as u64,
                PendingOp::Delete => 0,
            }
        }).sum();

        // 移除已 drain 的 key（而不是 clear，避免清除 drain 期间新写入的数据）
        for (key, _) in &items {
            self.pending.remove(key);
        }

        // 重新基于 map 实际大小设置计数器
        // 这样 drain 期间新写入的数据会被正确计数
        let actual_remaining = self.pending.len() as u64;
        self.pending_count.store(actual_remaining, Ordering::Release);

        // 重新计算剩余 bytes
        let actual_bytes: u64 = self.pending.iter()
            .map(|r| match r.value() {
                PendingOp::Put { value, .. } => value.len() as u64,
                PendingOp::Delete => 0,
            })
            .sum();
        self.pending_bytes.store(actual_bytes, Ordering::Release);

        items.into_iter().collect()
    }

    fn pending_len(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }

    fn pending_bytes(&self) -> u64 {
        self.pending_bytes.load(Ordering::Relaxed)
    }

    async fn wait_flush_trigger(&self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.flush_notify.notified()).await;
    }

    fn notify_flush(&self) {
        self.flush_notify.notify_one();
    }
}

/// Worker 节点：实际存储数据的节点
/// 支持两种模式：
/// 1. 单库模式：一个 KV + 一个 Meta（向后兼容）
/// 2. 分片模式：多个 KV + 多个 Meta（通过 ShardManager）
#[derive(Debug)]
pub struct WorkerNode {
    pub config: WorkerConfig,
    /// 单库模式下的 KvStore
    pub kv_store: Option<Arc<KvStore>>,
    /// 单库模式下的 MetaStore
    pub meta_store: Option<Arc<MetaStore>>,
    /// 分片模式下的 ShardManager
    pub shard_manager: Option<Arc<ShardManager>>,
    /// 写入统计（所有模式共享）
    write_stats: Arc<WriteStats>,
    /// 单库模式下的写合并缓冲区
    write_buffer: Option<Arc<WriteBuffer>>,
}

impl WorkerNode {
    /// 打开 Worker 节点（自动检测单库/分片模式）
    pub fn open(config: WorkerConfig) -> Result<Self> {
        let write_stats = Arc::new(WriteStats::new());

        // 如果配置了分片，使用 ShardManager
        if let Some(shard_configs) = &config.shard_configs {
            if !shard_configs.is_empty() {
                let manager = ShardManager::open(shard_configs.clone(), config.cache_size)?;
                manager.start_flusher(config.flush_interval_ms, 1000);
                return Ok(Self {
                    config,
                    kv_store: None,
                    meta_store: None,
                    shard_manager: Some(Arc::new(manager)),
                    write_stats,
                    write_buffer: None,
                });
            }
        }

        // 单库模式（向后兼容）
        if let Some(parent) = Path::new(&config.kv_path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let kv_store = Arc::new(KvStore::open(&config.kv_path)?);
        let meta_store = Arc::new(MetaStore::open(&config.meta_path)?);
        let write_buffer = Arc::new(WriteBuffer::new());

        Ok(Self {
            config,
            kv_store: Some(kv_store),
            meta_store: Some(meta_store),
            shard_manager: None,
            write_stats,
            write_buffer: Some(write_buffer),
        })
    }

    /// 判断是否使用分片模式
    pub fn is_sharded(&self) -> bool {
        self.shard_manager.is_some()
    }

    /// 获取写入统计快照（用于心跳上报）
    pub fn write_stats_snapshot(&self) -> WriteStatsSnapshot {
        self.write_stats.snapshot()
    }

    /// 启动单库模式的后台刷盘任务
    pub fn start_flusher(&self) {
        if let Some(wb) = &self.write_buffer {
            if let (Some(kv), Some(meta)) = (&self.kv_store, &self.meta_store) {
                let kv = kv.clone();
                let meta = meta.clone();
                let wb = wb.clone();
                let stats = self.write_stats.clone();
                let interval = Duration::from_millis(self.config.flush_interval_ms);

                println!("[flusher] 启动后台刷盘任务, interval={:?}", interval);

                tokio::spawn(async move {
                    loop {
                        // 等待触发：有新写入通知或 interval 超时
                        wb.wait_flush_trigger(interval).await;
                        // 抢占 flushing 标志，避免并发刷盘
                        while wb.flushing.swap(true, Ordering::AcqRel) {
                            tokio::task::yield_now().await;
                        }
                        let ops = wb.drain();
                        if ops.is_empty() {
                            wb.flushing.store(false, Ordering::Release);
                            continue;
                        }
                        let ops_count = ops.len();
                        let (count, bytes) = flush_ops(&kv, &meta, ops, &stats);
                        if count != ops_count as u64 {
                            eprintln!("[flusher] 刷盘完成 {}/{} 条, {} 字节", count, ops_count, bytes);
                        }
                        stats.record_flush(count, bytes);
                        wb.flushing.store(false, Ordering::Release);
                        // 处理完一批后立即检查是否有新数据（不等待 notify）
                        // 这样空闲时写入能立即落盘，连续写入时能批量处理
                        if wb.pending_len() > 0 {
                            continue;
                        }
                    }
                });
            } else {
                eprintln!("[flusher] 未启动: kv_store 或 meta_store 为 None");
            }
        } else {
            eprintln!("[flusher] 未启动: write_buffer 为 None (可能使用分片模式)");
        }
    }

    /// 获取 ShardManager 引用（分片模式）
    pub fn shard_manager(&self) -> Result<Arc<ShardManager>> {
        self.shard_manager.clone().ok_or_else(|| {
            crate::error::StoreError::InvalidArgument("Worker 未启用分片模式".to_string())
        })
    }


    // ========== KV 操作 ==========

    /// 写入对象（KV + Meta 一起缓冲，后台刷盘）
    /// 单库模式走写缓冲区；分片模式直接写盘并统计
    pub fn put_object(&self, key: &str, value: Bytes, meta: ObjectMeta) -> Result<()> {
        if let Some(wb) = &self.write_buffer {
            // 单库模式：提交到写缓冲区
            wb.submit_put(key.to_string(), value, meta, &self.write_stats);
            Ok(())
        } else if self.shard_manager.is_some() {
            // 分片模式：直接写盘，统计
            let byte_len = value.len() as u64;
            self.put(key, value)?;
            self.put_meta(&meta)?;
            self.write_stats.record_put(byte_len);
            self.write_stats.record_flush(1, byte_len);
            Ok(())
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    /// 批量写入对象（KV + Meta 一起缓冲）
    pub fn put_objects_batch(&self, items: Vec<(String, Bytes, ObjectMeta)>) -> Result<()> {
        if let Some(wb) = &self.write_buffer {
            // 批量提交到缓冲区，只触发一次 notify
            let n = items.len();
            for (key, value, meta) in items {
                wb.submit_put_silent(key, value, meta, &self.write_stats);
            }
            // 批量提交完成后只通知一次 flusher
            wb.notify_flush();
            let _ = n;
            Ok(())
        } else if self.shard_manager.is_some() {
            let mut kvs = Vec::with_capacity(items.len());
            let mut metas = Vec::with_capacity(items.len());
            let mut total_bytes = 0u64;
            for (key, value, meta) in items {
                total_bytes += value.len() as u64;
                kvs.push((key, value));
                metas.push(meta);
            }
            self.put_batch(kvs)?;
            self.put_meta_batch(&metas)?;
            let count = metas.len() as u64;
            self.write_stats.record_put(count);
            self.write_stats.record_flush(count, total_bytes);
            Ok(())
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    /// 读取对象（先查写缓冲区中未刷盘的数据，再查磁盘）
    pub fn get_object(&self, key: &str) -> Result<Option<(Bytes, Option<ObjectMeta>)>> {
        // 先查写缓冲区
        if let Some(wb) = &self.write_buffer {
            if let Some(op) = wb.pending.get(key) {
                match op.value() {
                    PendingOp::Put { value, meta } => {
                        return Ok(Some((value.clone(), Some(meta.clone()))));
                    }
                    PendingOp::Delete => return Ok(None),
                }
            }
        }
        // 查磁盘
        let value = self.get(key)?;
        if let Some(v) = value {
            let meta = self.get_meta(key).ok();
            Ok(Some((v, meta)))
        } else if let Some(wb) = &self.write_buffer {
            // 缓冲区和磁盘都没找到：可能正在刷盘中（drain 之后、flush_ops 完成之前）
            // 等待刷盘完成后再查一次磁盘
            let mut waited = 0u64;
            while wb.flushing.load(Ordering::Acquire) && waited < 100 {
                std::thread::sleep(std::time::Duration::from_millis(1));
                waited += 1;
            }
            if waited > 0 {
                let value = self.get(key)?;
                if let Some(v) = value {
                    let meta = self.get_meta(key).ok();
                    return Ok(Some((v, meta)));
                }
            }
            Ok(None)
        } else {
            Ok(None)
        }
    }

    /// 删除对象（提交到写缓冲区）
    pub fn delete_object(&self, key: &str) -> Result<()> {
        if let Some(wb) = &self.write_buffer {
            wb.submit_delete(key.to_string(), &self.write_stats);
            Ok(())
        } else {
            self.delete(key)?;
            self.write_stats.record_put(0);
            self.write_stats.record_flush(1, 0);
            Ok(())
        }
    }

    pub fn put(&self, key: &str, value: Bytes) -> Result<()> {
        if let Some(sm) = &self.shard_manager {
            let shard_idx = sm.route(key);
            let shard = sm.shard(shard_idx)?;
            shard.kv_store.put(key, value)
        } else if let Some(kv) = &self.kv_store {
            kv.put(key, value)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn get(&self, key: &str) -> Result<Option<Bytes>> {
        if let Some(sm) = &self.shard_manager {
            let shard_idx = sm.route(key);
            let shard = sm.shard(shard_idx)?;
            shard.kv_store.get(key)
        } else if let Some(kv) = &self.kv_store {
            kv.get(key)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        if let Some(sm) = &self.shard_manager {
            let shard_idx = sm.route(key);
            let shard = sm.shard(shard_idx)?;
            shard.kv_store.delete(key)
        } else if let Some(kv) = &self.kv_store {
            kv.delete(key)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        // 先查写缓冲区
        if let Some(wb) = &self.write_buffer {
            if let Some(op) = wb.pending.get(key) {
                return Ok(matches!(op.value(), PendingOp::Put { .. }));
            }
        }
        if let Some(sm) = &self.shard_manager {
            let shard_idx = sm.route(key);
            let shard = sm.shard(shard_idx)?;
            shard.kv_store.exists(key)
        } else if let Some(kv) = &self.kv_store {
            kv.exists(key)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn scan(&self, prefix: &str, limit: usize) -> Result<Vec<(String, Bytes)>> {
        if let Some(sm) = &self.shard_manager {
            // 分片模式：扫描所有分片
            let mut results = Vec::new();
            for shard in sm.shards() {
                let items = shard.kv_store.scan(prefix, limit)?;
                results.extend(items);
                if results.len() >= limit {
                    results.truncate(limit);
                    break;
                }
            }
            Ok(results)
        } else if let Some(kv) = &self.kv_store {
            kv.scan(prefix, limit)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn put_batch(&self, kvs: Vec<(String, Bytes)>) -> Result<()> {
        if let Some(sm) = &self.shard_manager {
            // 按分片分组
            let mut grouped: Vec<Vec<(String, Bytes)>> = vec![Vec::new(); sm.num_shards()];
            for (key, value) in kvs {
                let idx = sm.route(&key);
                grouped[idx].push((key, value));
            }
            for (idx, batch) in grouped.iter().enumerate() {
                if !batch.is_empty() {
                    let shard = sm.shard(idx)?;
                    shard.kv_store.put_batch(batch.clone())?;
                }
            }
            Ok(())
        } else if let Some(kv) = &self.kv_store {
            kv.put_batch(kvs)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn delete_batch(&self, keys: Vec<String>) -> Result<()> {
        if let Some(sm) = &self.shard_manager {
            let mut grouped: Vec<Vec<String>> = vec![Vec::new(); sm.num_shards()];
            for key in keys {
                let idx = sm.route(&key);
                grouped[idx].push(key);
            }
            for (idx, batch) in grouped.iter().enumerate() {
                if !batch.is_empty() {
                    let shard = sm.shard(idx)?;
                    shard.kv_store.delete_batch(batch.clone())?;
                }
            }
            Ok(())
        } else if let Some(kv) = &self.kv_store {
            kv.delete_batch(keys)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    // ========== Meta 操作 ==========

    pub fn put_meta(&self, meta: &ObjectMeta) -> Result<()> {
        if let Some(sm) = &self.shard_manager {
            let shard_idx = sm.route(&meta.key);
            let shard = sm.shard(shard_idx)?;
            shard.meta_store.put(meta)
        } else if let Some(meta_store) = &self.meta_store {
            meta_store.put(meta)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn get_meta(&self, key: &str) -> Result<ObjectMeta> {
        if let Some(sm) = &self.shard_manager {
            let shard_idx = sm.route(key);
            let shard = sm.shard(shard_idx)?;
            shard.meta_store.get(key)
        } else if let Some(meta_store) = &self.meta_store {
            meta_store.get(key)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn delete_meta(&self, key: &str) -> Result<()> {
        if let Some(sm) = &self.shard_manager {
            let shard_idx = sm.route(key);
            let shard = sm.shard(shard_idx)?;
            shard.meta_store.delete(key)
        } else if let Some(meta_store) = &self.meta_store {
            meta_store.delete(key)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn meta_exists(&self, key: &str) -> Result<bool> {
        if let Some(sm) = &self.shard_manager {
            let shard_idx = sm.route(key);
            let shard = sm.shard(shard_idx)?;
            shard.meta_store.exists(key)
        } else if let Some(meta_store) = &self.meta_store {
            meta_store.exists(key)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn list_meta(&self, prefix: &str, limit: usize) -> Result<Vec<ObjectMeta>> {
        if let Some(sm) = &self.shard_manager {
            // 分片模式：扫描所有分片的 meta
            let mut all_metas = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for shard in sm.shards() {
                let metas = shard.meta_store.list(prefix, limit)?;
                for m in metas {
                    if seen.insert(m.key.clone()) {
                        all_metas.push(m);
                    }
                }
            }
            all_metas.sort_by(|a, b| a.key.cmp(&b.key));
            all_metas.truncate(limit);
            Ok(all_metas)
        } else if let Some(meta_store) = &self.meta_store {
            meta_store.list(prefix, limit)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn put_meta_batch(&self, metas: &[ObjectMeta]) -> Result<()> {
        if let Some(sm) = &self.shard_manager {
            let mut grouped: Vec<Vec<ObjectMeta>> = vec![Vec::new(); sm.num_shards()];
            for meta in metas {
                let idx = sm.route(&meta.key);
                grouped[idx].push(meta.clone());
            }
            for (idx, batch) in grouped.iter().enumerate() {
                if !batch.is_empty() {
                    let shard = sm.shard(idx)?;
                    shard.meta_store.put_batch_txn(batch)?;
                }
            }
            Ok(())
        } else if let Some(meta_store) = &self.meta_store {
            meta_store.put_batch_txn(metas)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }

    pub fn delete_meta_batch(&self, keys: &[String]) -> Result<()> {
        if let Some(sm) = &self.shard_manager {
            let mut grouped: Vec<Vec<String>> = vec![Vec::new(); sm.num_shards()];
            for key in keys {
                let idx = sm.route(key);
                grouped[idx].push(key.clone());
            }
            for (idx, batch) in grouped.iter().enumerate() {
                if !batch.is_empty() {
                    let shard = sm.shard(idx)?;
                    shard.meta_store.delete_batch_txn(batch)?;
                }
            }
            Ok(())
        } else if let Some(meta_store) = &self.meta_store {
            meta_store.delete_batch_txn(keys)
        } else {
            Err(crate::error::StoreError::InvalidArgument("Worker 未初始化".to_string()))
        }
    }
}

/// 将缓冲区中的操作刷盘到 KvStore + MetaStore
/// 返回 (成功刷盘的操作数, 成功刷盘的字节数)
fn flush_ops(
    kv: &KvStore,
    meta: &MetaStore,
    ops: HashMap<String, PendingOp>,
    stats: &WriteStats,
) -> (u64, u64) {
    let mut count = 0u64;
    let mut bytes = 0u64;

    // 分离 put 和 delete 操作，批量提交以减少事务开销
    let mut put_kvs: Vec<(String, Bytes)> = Vec::new();
    let mut put_metas: Vec<ObjectMeta> = Vec::new();
    let mut del_keys: Vec<String> = Vec::new();

    for (key, op) in ops {
        match op {
            PendingOp::Put { value, meta: obj_meta } => {
                bytes += value.len() as u64;
                put_kvs.push((key, value));
                put_metas.push(obj_meta);
            }
            PendingOp::Delete => {
                del_keys.push(key);
            }
        }
    }

    // 批量写入 KV
    if !put_kvs.is_empty() {
        let n = put_kvs.len();
        match kv.put_batch(put_kvs) {
            Ok(_) => count += n as u64,
            Err(e) => eprintln!("[flusher] 批量 put KV 失败 {} 条: {}", n, e),
        }
    }

    // 批量写入 Meta
    if !put_metas.is_empty() {
        let n = put_metas.len();
        match meta.put_batch_txn(&put_metas) {
            Ok(_) => {} // count 已在 KV 批量写入时统计
            Err(e) => eprintln!("[flusher] 批量 put meta 失败 {} 条: {}", n, e),
        }
    }

    // 批量删除
    if !del_keys.is_empty() {
        let n = del_keys.len();
        // KV 批量删除
        if let Err(e) = kv.delete_batch(del_keys.clone()) {
            eprintln!("[flusher] 批量 delete KV 失败 {} 条: {}", n, e);
        }
        // Meta 批量删除
        match meta.delete_batch_txn(&del_keys) {
            Ok(_) => count += n as u64,
            Err(e) => eprintln!("[flusher] 批量 delete meta 失败 {} 条: {}", n, e),
        }
    }

    // 注意：record_flush 在 flusher 循环中调用，这里只返回统计值
    let _ = stats; // 避免未使用警告
    (count, bytes)
}


/// Worker gRPC 服务实现
#[derive(Debug, Clone)]
pub struct WorkerService {
    node: Arc<WorkerNode>,
}

impl WorkerService {
    pub fn new(node: WorkerNode) -> Self {
        Self {
            node: Arc::new(node),
        }
    }

    pub fn new_arc(node: Arc<WorkerNode>) -> Self {
        Self {
            node,
        }
    }

    fn convert_meta(meta: ObjectMeta) -> proto::ObjectMeta {
        proto::ObjectMeta {
            key: meta.key,
            size: meta.size,
            created_at: meta.created_at.to_rfc3339(),
            updated_at: meta.updated_at.to_rfc3339(),
            content_type: meta.content_type.unwrap_or_default(),
            tags: meta.tags.map(|t| t.to_string()).unwrap_or_default(),
    }
    }
}

#[tonic::async_trait]
impl proto::worker_service_server::WorkerService for WorkerService {
    async fn put(&self, request: Request<proto::PutRequest>) -> std::result::Result<Response<proto::PutResponse>, Status> {
        let req = request.into_inner();
        let value = Bytes::from(req.value);

        let now = Utc::now();
        let meta = ObjectMeta {
            key: req.key.clone(),
            size: value.len() as u64,
            created_at: now,
            updated_at: now,
            content_type: if req.content_type.is_empty() { None } else { Some(req.content_type) },
            tags: if req.tags.is_empty() { None } else { Some(serde_json::from_str(&req.tags).map_err(|e| Status::invalid_argument(format!("Invalid tags: {}", e)))?) },
            checksum: None,
            storage_node: None,
        };


        self.node.put_object(&req.key, value, meta.clone()).map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::PutResponse {
            meta: Some(Self::convert_meta(meta)),
        }))
    }

    async fn get(&self, request: Request<proto::GetRequest>) -> std::result::Result<Response<proto::GetResponse>, Status> {
        let req = request.into_inner();

        let (value, meta) = self.node.get_object(&req.key)
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found(format!("Key not found: {}", req.key)))?;

        Ok(Response::new(proto::GetResponse {
            value: value.to_vec(),
            meta: meta.map(Self::convert_meta),
        }))
    }

    async fn delete(&self, request: Request<proto::DeleteRequest>) -> std::result::Result<Response<proto::DeleteResponse>, Status> {
        let req = request.into_inner();

        self.node.delete_object(&req.key).map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::DeleteResponse { success: true }))
    }

    async fn exists(&self, request: Request<proto::ExistsRequest>) -> std::result::Result<Response<proto::ExistsResponse>, Status> {
        let req = request.into_inner();

        let exists = self.node.meta_exists(&req.key).map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(proto::ExistsResponse { exists }))
    }

    async fn list(&self, request: Request<proto::ListRequest>) -> std::result::Result<Response<proto::ListResponse>, Status> {
        let req = request.into_inner();

        let metas = self.node.list_meta(&req.prefix, req.limit as usize)
            .map_err(|e| Status::internal(e.to_string()))?;

        let proto_metas = metas.into_iter().map(Self::convert_meta).collect();

        Ok(Response::new(proto::ListResponse { metas: proto_metas }))
    }

    async fn put_batch(&self, request: Request<proto::PutBatchRequest>) -> std::result::Result<Response<proto::PutBatchResponse>, Status> {
        let req = request.into_inner();
        let now = Utc::now();

        let mut items = Vec::with_capacity(req.items.len());
        let mut metas = Vec::with_capacity(req.items.len());

        for item in req.items {
            let value = Bytes::from(item.value);
            let meta = ObjectMeta {
                key: item.key.clone(),
                size: value.len() as u64,
                created_at: now,
                updated_at: now,
                content_type: if item.content_type.is_empty() { None } else { Some(item.content_type) },
                tags: if item.tags.is_empty() { None } else { Some(serde_json::from_str(&item.tags).map_err(|e| Status::invalid_argument(format!("Invalid tags: {}", e)))?) },
                checksum: None,
                storage_node: None,
            };
            metas.push(meta.clone());
            items.push((item.key, value, meta));
        }


        self.node.put_objects_batch(items).map_err(|e| Status::internal(e.to_string()))?;

        let proto_metas = metas.into_iter().map(Self::convert_meta).collect();

        Ok(Response::new(proto::PutBatchResponse { metas: proto_metas }))
    }
}
