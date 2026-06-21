use crate::error::{Result, StoreError};
use crate::kv::KvStore;
use crate::meta::{MetaStore, ObjectMeta};
use bytes::Bytes;
use dashmap::DashMap;
use moka::sync::Cache as MokaCache;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// 分片配置：定义单个分片的数据库文件路径、名称和扩展名
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// 分片编号（从 0 开始）
    pub shard_id: usize,
    /// 数据库文件存放目录
    pub data_dir: PathBuf,
    /// KV 数据库文件名（不含扩展名）
    pub kv_name: String,
    /// KV 数据库文件扩展名（含点号，如 ".db"）
    pub kv_ext: String,
    /// Meta 数据库文件名（不含扩展名）
    pub meta_name: String,
    /// Meta 数据库文件扩展名（含点号，如 ".db"）
    pub meta_ext: String,
}

impl ShardConfig {
    /// 创建默认配置
    pub fn new(shard_id: usize, data_dir: impl AsRef<Path>) -> Self {
        Self {
            shard_id,
            data_dir: data_dir.as_ref().to_path_buf(),
            kv_name: format!("kv_{}", shard_id),
            kv_ext: ".db".to_string(),
            meta_name: format!("meta_{}", shard_id),
            meta_ext: ".db".to_string(),
        }
    }

    /// 自定义 KV 数据库文件名和扩展名
    pub fn with_kv(mut self, name: impl Into<String>, ext: impl Into<String>) -> Self {
        self.kv_name = name.into();
        self.kv_ext = ext.into();
        self
    }

    /// 自定义 Meta 数据库文件名和扩展名
    pub fn with_meta(mut self, name: impl Into<String>, ext: impl Into<String>) -> Self {
        self.meta_name = name.into();
        self.meta_ext = ext.into();
        self
    }

    /// 获取 KV 数据库完整路径
    pub fn kv_path(&self) -> PathBuf {
        let mut path = self.data_dir.clone();
        path.push(format!("{}{}", self.kv_name, self.kv_ext));
        path
    }

    /// 获取 Meta 数据库完整路径
    pub fn meta_path(&self) -> PathBuf {
        let mut path = self.data_dir.clone();
        path.push(format!("{}{}", self.meta_name, self.meta_ext));
        path
    }
}

/// 分片实例：包含一个 KvStore 和一个 MetaStore
#[derive(Debug)]
pub struct Shard {
    pub config: ShardConfig,
    pub kv_store: Arc<KvStore>,
    pub meta_store: Arc<MetaStore>,
}

impl Shard {
    pub fn open(config: ShardConfig) -> Result<Self> {
        // 确保数据目录存在
        std::fs::create_dir_all(&config.data_dir)?;

        let kv_store = Arc::new(KvStore::open(config.kv_path())?);
        let meta_store = Arc::new(MetaStore::open(config.meta_path())?);

        Ok(Self {
            config,
            kv_store,
            meta_store,
        })
    }
}

/// 待刷盘的写操作
#[derive(Debug, Clone)]
enum PendingOp {
    Put { value: Bytes, meta: ObjectMeta },
    Delete,
}

/// 写合并缓冲区：put/delete 先入内存，后台批量刷盘
struct WriteBuffer {
    /// 待刷盘的写操作（按 key 去重，后写覆盖先写）
    pending: DashMap<String, PendingOp>,
    /// 待刷盘操作计数（含删除），用于触发阈值刷盘
    pending_count: AtomicU64,
    /// 刷盘通知
    flush_notify: Notify,
    /// 是否正在刷盘（防止后台任务与 flush() 冲突）
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
            flush_notify: Notify::new(),
            flushing: AtomicBool::new(false),
        }
    }

    /// 写入缓冲区（立即返回，不阻塞）
    fn submit_put(&self, key: String, value: Bytes, meta: ObjectMeta) {
        self.pending.insert(key, PendingOp::Put { value, meta });
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        // 通知 flusher 立即处理：空闲时立即落盘，不等待 interval 超时
        self.notify_flush();
    }

    fn submit_delete(&self, key: String) {
        self.pending.insert(key, PendingOp::Delete);
        self.pending_count.fetch_add(1, Ordering::Relaxed);
        // 通知 flusher 立即处理
        self.notify_flush();
    }

    /// 取出所有待刷盘操作（drain 语义）
    fn drain(&self) -> HashMap<String, PendingOp> {
        // 先收集所有 key（快照）
        let keys: Vec<String> = self.pending.iter().map(|r| r.key().clone()).collect();
        let mut result = HashMap::with_capacity(keys.len());
        // 逐 key 移除：drain 期间新写入的 key 不会被错误清除
        for key in keys {
            if let Some((k, v)) = self.pending.remove(&key) {
                result.insert(k, v);
            }
        }
        // 重新计算实际剩余量，而不是直接置 0
        self.pending_count
            .store(self.pending.len() as u64, Ordering::Release);
        result
    }

    fn pending_len(&self) -> u64 {
        self.pending_count.load(Ordering::Relaxed)
    }

    /// 等待下一次刷盘触发（超时或显式通知）
    async fn wait_flush_trigger(&self, timeout: std::time::Duration) {
        let _ = tokio::time::timeout(timeout, self.flush_notify.notified()).await;
    }

    fn notify_flush(&self) {
        self.flush_notify.notify_one();
    }
}

/// Type alias for a custom shard-routing function.
type ShardRouteFn = Arc<dyn Fn(&str, usize) -> usize + Send + Sync>;

/// 分片路由策略
#[derive(Clone, Default)]
pub enum ShardStrategy {
    /// 基于 key 哈希取模路由
    #[default]
    Hash,
    /// 自定义路由函数（Arc 包装以便跨线程共享）
    Custom(ShardRouteFn),
}

impl std::fmt::Debug for ShardStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShardStrategy::Hash => f.debug_tuple("Hash").finish(),
            ShardStrategy::Custom(_) => f.debug_tuple("Custom").field(&"<fn>").finish(),
        }
    }
}

/// 分布式存储管理器：管理多个分片，提供统一的读写接口
#[derive(Debug)]
pub struct ShardManager {
    /// 所有分片
    shards: Vec<Shard>,
    /// 路由策略
    strategy: ShardStrategy,
    /// 写合并缓冲区（按分片索引）
    write_buffers: Vec<Arc<WriteBuffer>>,
    /// 热点数据缓存（LRU，按分片索引）
    caches: Vec<MokaCache<String, Bytes>>,
    /// 缓存大小限制（每个分片）
    cache_size: usize,
    /// 后台刷盘任务句柄
    flush_handles: Arc<tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl ShardManager {
    /// 使用分片配置列表创建 ShardManager
    pub fn open(configs: Vec<ShardConfig>, cache_size: usize) -> Result<Self> {
        if configs.is_empty() {
            return Err(StoreError::InvalidArgument(
                "至少需要一个分片配置".to_string(),
            ));
        }

        let mut shards = Vec::with_capacity(configs.len());
        for config in configs {
            shards.push(Shard::open(config)?);
        }

        // 每个分片独立进行 WAL 崩溃恢复
        for shard in &shards {
            let wal_entries = shard.meta_store.list_wal_entries()?;
            if !wal_entries.is_empty() {
                eprintln!(
                    "[recovery] 分片 {} 发现 {} 条 WAL 记录，开始恢复...",
                    shard.config.shard_id,
                    wal_entries.len()
                );
                for entry in wal_entries {
                    match entry.op_type.as_str() {
                        "put" => {
                            if shard.kv_store.exists(&entry.key).unwrap_or(false) {
                                if let Some(ref json) = entry.meta_json {
                                    if let Ok(m) = serde_json::from_str::<ObjectMeta>(json) {
                                        let _ = shard.meta_store.put(&m);
                                    }
                                }
                            }
                        }
                        "delete" => {
                            let _ = shard.kv_store.delete(&entry.key);
                            let _ = shard.meta_store.delete(&entry.key);
                        }
                        _ => {}
                    }
                    let _ = shard.meta_store.delete_wal_entry(&entry.key);
                }
                eprintln!("[recovery] 分片 {} WAL 恢复完成", shard.config.shard_id);
            }
        }

        let num_shards = shards.len();
        let mut write_buffers = Vec::with_capacity(num_shards);
        let mut caches = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            write_buffers.push(Arc::new(WriteBuffer::new()));
            caches.push(
                MokaCache::builder()
                    .max_capacity(cache_size as u64)
                    .build(),
            );
        }

        Ok(Self {
            shards,
            strategy: ShardStrategy::Hash,
            write_buffers,
            caches,
            cache_size,
            flush_handles: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        })
    }

    /// 使用便捷方式创建：指定数据目录、分片数量、文件名模板
    pub fn open_simple(
        data_dir: impl AsRef<Path>,
        num_shards: usize,
        kv_name_template: &str,   // 如 "kv_{}"，{} 会被替换为分片编号
        kv_ext: &str,             // 如 ".db"
        meta_name_template: &str, // 如 "meta_{}"
        meta_ext: &str,           // 如 ".db"
        cache_size: usize,
    ) -> Result<Self> {
        if num_shards == 0 {
            return Err(StoreError::InvalidArgument(
                "分片数量必须大于 0".to_string(),
            ));
        }

        let configs: Vec<ShardConfig> = (0..num_shards)
            .map(|i| {
                ShardConfig::new(i, data_dir.as_ref())
                    .with_kv(kv_name_template.replace("{}", &i.to_string()), kv_ext)
                    .with_meta(meta_name_template.replace("{}", &i.to_string()), meta_ext)
            })
            .collect();

        Self::open(configs, cache_size)
    }

    /// 设置路由策略
    pub fn set_strategy(&mut self, strategy: ShardStrategy) {
        self.strategy = strategy;
    }

    /// 获取分片数量
    pub fn num_shards(&self) -> usize {
        self.shards.len()
    }

    /// 根据 key 路由到目标分片索引
    pub fn route(&self, key: &str) -> usize {
        match &self.strategy {
            ShardStrategy::Hash => {
                let hash = seahash::hash(key.as_bytes());
                (hash as usize) % self.shards.len()
            }
            ShardStrategy::Custom(f) => (f)(key, self.shards.len()),
        }
    }

    /// 获取指定分片的引用
    pub fn shard(&self, index: usize) -> Result<&Shard> {
        self.shards.get(index).ok_or_else(|| {
            StoreError::InvalidArgument(format!(
                "分片索引 {} 超出范围 (0..{})",
                index,
                self.shards.len()
            ))
        })
    }

    /// 获取所有分片的引用
    pub fn shards(&self) -> &[Shard] {
        &self.shards
    }

    /// 启动后台批量刷盘任务
    pub fn start_flusher(&self, interval_ms: u64, _threshold: usize) {
        let num_shards = self.shards.len();
        let mut handles = Vec::with_capacity(num_shards);

        for shard_idx in 0..num_shards {
            let kv = self.shards[shard_idx].kv_store.clone();
            let meta = self.shards[shard_idx].meta_store.clone();
            let cache = self.caches[shard_idx].clone();
            let cache_size = self.cache_size;
            let wb = self.write_buffers[shard_idx].clone();
            let interval = std::time::Duration::from_millis(interval_ms);

            let handle = tokio::spawn(async move {
                loop {
                    wb.wait_flush_trigger(interval).await;

                    // 等待其他 flush 完成
                    while wb.flushing.swap(true, Ordering::AcqRel) {
                        tokio::task::yield_now().await;
                    }

                    let ops = wb.drain();
                    if ops.is_empty() {
                        wb.flushing.store(false, Ordering::Release);
                        continue;
                    }

                    // 执行批量刷盘
                    if let Err(e) = flush_ops(&kv, &meta, &cache, cache_size, ops) {
                        eprintln!("[flusher-{}] 批量刷盘失败: {}", shard_idx, e);
                    }

                    wb.flushing.store(false, Ordering::Release);
                    // 处理完一批后立即检查是否有新数据（不等待 notify）
                    // 这样空闲时写入能立即落盘，连续写入时能批量处理
                    if wb.pending_len() > 0 {
                        continue;
                    }
                }
            });

            handles.push(handle);
        }

        // 保存句柄
        let handles_arc = self.flush_handles.clone();
        tokio::spawn(async move {
            *handles_arc.lock().await = handles;
        });
    }

    /// 同步刷盘：等待所有 pending 操作落盘
    pub async fn flush(&self) -> Result<()> {
        for wb in self.write_buffers.iter() {
            loop {
                wb.notify_flush();
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                if wb.pending_len() == 0 && !wb.flushing.load(Ordering::Acquire) {
                    break;
                }
            }
        }
        Ok(())
    }

    /// 写入对象
    pub async fn put(
        &self,
        key: String,
        value: Bytes,
        content_type: Option<String>,
        tags: Option<serde_json::Value>,
    ) -> Result<ObjectMeta> {
        let shard_idx = self.route(&key);
        let cache = &self.caches[shard_idx];
        let wb = &self.write_buffers[shard_idx];

        let now = chrono::Utc::now();
        let meta = ObjectMeta {
            key: key.clone(),
            size: value.len() as u64,
            created_at: now,
            updated_at: now,
            content_type,
            tags,
            checksum: None,
            storage_node: None,
        };

        // 写入缓存（moka 自动 LRU 淘汰，无需手动判断容量）
        cache.insert(key.clone(), value.clone());

        // 写入合并缓冲区
        wb.submit_put(key, value, meta.clone());

        Ok(meta)
    }

    /// 读取对象
    pub async fn get(&self, key: &str) -> Result<(Bytes, ObjectMeta)> {
        let shard_idx = self.route(key);
        let cache = &self.caches[shard_idx];
        let wb = &self.write_buffers[shard_idx];
        let shard = &self.shards[shard_idx];

        // 先查缓存
        if let Some(cached) = cache.get(key) {
            match shard.meta_store.get(key) {
                Ok(meta) => return Ok((cached.clone(), meta)),
                Err(_) => {
                    if let Some(op) = wb.pending.get(key) {
                        if let PendingOp::Put { meta, .. } = op.value() {
                            return Ok((cached.clone(), meta.clone()));
                        }
                    }
                    // 等待刷盘完成再试
                    self.flush().await?;
                    let meta = shard.meta_store.get(key)?;
                    return Ok((cached.clone(), meta));
                }
            }
        }

        // 查 pending
        if let Some(op) = wb.pending.get(key) {
            if let PendingOp::Put { value, meta } = op.value() {
                let value = value.clone();
                let meta = meta.clone();
                drop(op);
                cache.insert(key.to_string(), value.clone());
                return Ok((value, meta));
            }
        }

        // 查磁盘
        let value = shard
            .kv_store
            .get(key)?
            .ok_or_else(|| StoreError::KeyNotFound(key.to_string()))?;
        let meta = shard.meta_store.get(key)?;

        cache.insert(key.to_string(), value.clone());

        Ok((value, meta))
    }

    /// 删除对象
    pub async fn delete(&self, key: &str) -> Result<()> {
        let shard_idx = self.route(key);
        let cache = &self.caches[shard_idx];
        let wb = &self.write_buffers[shard_idx];

        cache.invalidate(key);
        wb.submit_delete(key.to_string());
        Ok(())
    }

    /// 检查对象是否存在
    pub async fn exists(&self, key: &str) -> Result<bool> {
        let shard_idx = self.route(key);
        let cache = &self.caches[shard_idx];
        let wb = &self.write_buffers[shard_idx];
        let shard = &self.shards[shard_idx];

        if let Some(op) = wb.pending.get(key) {
            return Ok(matches!(op.value(), PendingOp::Put { .. }));
        }
        if cache.get(key).is_some() {
            return Ok(true);
        }
        shard.meta_store.exists(key)
    }

    /// 按前缀列出对象（跨所有分片扫描）
    pub async fn list(&self, prefix: &str, limit: usize) -> Result<Vec<ObjectMeta>> {
        let mut all_metas = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

        for (shard_idx, shard) in self.shards.iter().enumerate() {
            let wb = &self.write_buffers[shard_idx];

            // 从磁盘获取
            let metas = shard.meta_store.list(prefix, limit)?;
            for m in metas {
                if seen.insert(m.key.clone()) {
                    all_metas.push(m);
                }
            }

            // 合并 pending 中的更新
            for entry in wb.pending.iter() {
                let key = entry.key();
                if key.starts_with(prefix) {
                    match entry.value() {
                        PendingOp::Put { meta, .. } => {
                            if seen.insert(key.clone()) {
                                all_metas.push(meta.clone());
                            }
                        }
                        PendingOp::Delete => {
                            seen.remove(key);
                        }
                    }
                }
            }
        }

        // 过滤掉被删除的
        all_metas.retain(|m| seen.contains(&m.key));
        all_metas.sort_by(|a, b| a.key.cmp(&b.key));
        all_metas.truncate(limit);
        Ok(all_metas)
    }

    /// 批量写入对象
    pub async fn put_batch(
        &self,
        items: Vec<(String, Bytes, Option<String>, Option<serde_json::Value>)>,
    ) -> Result<Vec<ObjectMeta>> {
        let mut all_metas = Vec::with_capacity(items.len());
        let now = chrono::Utc::now();

        for (key, value, content_type, tags) in items {
            let shard_idx = self.route(&key);
            let cache = &self.caches[shard_idx];
            let wb = &self.write_buffers[shard_idx];

            let meta = ObjectMeta {
                key: key.clone(),
                size: value.len() as u64,
                created_at: now,
                updated_at: now,
                content_type,
                tags,
                checksum: None,
                storage_node: None,
            };

            cache.insert(key.clone(), value.clone());
            wb.submit_put(key, value, meta.clone());
            all_metas.push(meta);
        }

        Ok(all_metas)
    }

    /// 获取指定分片的 KvStore 引用
    pub fn kv_store(&self, shard_idx: usize) -> Result<Arc<KvStore>> {
        Ok(self
            .shards
            .get(shard_idx)
            .ok_or_else(|| StoreError::InvalidArgument(format!("分片索引 {} 超出范围", shard_idx)))?
            .kv_store
            .clone())
    }

    /// 获取指定分片的 MetaStore 引用
    pub fn meta_store(&self, shard_idx: usize) -> Result<Arc<MetaStore>> {
        Ok(self
            .shards
            .get(shard_idx)
            .ok_or_else(|| StoreError::InvalidArgument(format!("分片索引 {} 超出范围", shard_idx)))?
            .meta_store
            .clone())
    }
}

/// 执行批量刷盘：将 pending 操作一次性写入 jammdb + SQLite（WAL 三步协议）
fn flush_ops(
    kv: &Arc<KvStore>,
    meta: &Arc<MetaStore>,
    _cache: &MokaCache<String, Bytes>,
    _cache_size: usize,
    ops: HashMap<String, PendingOp>,
) -> Result<()> {
    if ops.is_empty() {
        return Ok(());
    }

    let mut put_kvs: Vec<(String, Bytes)> = Vec::new();
    let mut put_metas: Vec<ObjectMeta> = Vec::new();
    let mut del_keys: Vec<String> = Vec::new();

    for (key, op) in ops {
        match op {
            PendingOp::Put { value, meta: obj_meta } => {
                put_kvs.push((key, value));
                put_metas.push(obj_meta);
            }
            PendingOp::Delete => {
                del_keys.push(key);
            }
        }
    }

    // 步骤 1：写 WAL 意图记录
    let wal_puts: Vec<(String, String)> = put_metas
        .iter()
        .map(|m| (m.key.clone(), serde_json::to_string(m).unwrap_or_default()))
        .collect();
    meta.write_wal_batch(&wal_puts, &del_keys)?;

    // 步骤 2：写 KV
    if !put_kvs.is_empty() {
        kv.put_batch(put_kvs)?;
    }
    if !del_keys.is_empty() {
        let _ = kv.delete_batch(del_keys.clone());
    }

    // 步骤 3：写 Meta + 原子清除 WAL
    meta.commit_meta_clear_wal(&put_metas, &del_keys)?;

    Ok(())
}
