use crate::error::{Result, StoreError};
use crate::kv::KvStore;
use crate::meta::{MetaStore, ObjectMeta};
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use moka::sync::Cache as MokaCache;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

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
        let keys: Vec<String> = self.pending.iter().map(|r| r.key().clone()).collect();
        let mut result = HashMap::with_capacity(keys.len());
        for key in keys {
            if let Some((k, v)) = self.pending.remove(&key) {
                result.insert(k, v);
            }
        }
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

/// 核心存储层：整合 KV + Meta + LRU 缓存 + 写合并缓冲区
#[derive(Debug, Clone)]
pub struct Store {
    kv_store: Arc<KvStore>,
    meta_store: Arc<MetaStore>,
    cache: MokaCache<String, Bytes>, // LRU 热点缓存，自动淘汰
    /// 写合并缓冲区
    write_buffer: Arc<WriteBuffer>,
    /// 后台刷盘任务句柄（用于关闭时等待）
    flush_handle: Arc<tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl Store {
    /// 打开或创建存储，初始化 KV/Meta/Cache/WriteBuffer
    pub fn open<P: AsRef<Path>>(kv_path: P, meta_path: P, cache_size: usize) -> Result<Self> {
        let kv_store = Arc::new(KvStore::open(kv_path)?);
        let meta_store = Arc::new(MetaStore::open(meta_path)?);
        let cache = MokaCache::builder()
            .max_capacity(cache_size as u64)
            .build();
        let write_buffer = Arc::new(WriteBuffer::new());

        Ok(Self {
            kv_store,
            meta_store,
            cache,
            write_buffer,
            flush_handle: Arc::new(tokio::sync::Mutex::new(None)),
        })
    }

    /// 启动后台批量刷盘任务
    pub fn start_flusher(&self, interval_ms: u64) {
        let kv = self.kv_store.clone();
        let meta = self.meta_store.clone();
        let cache = self.cache.clone();
        let wb = self.write_buffer.clone();
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
                if let Err(e) = flush_ops(&kv, &meta, &cache, 0, ops) {
                    eprintln!("[flusher] 批量刷盘失败: {}", e);
                }

                wb.flushing.store(false, Ordering::Release);
                // 处理完一批后立即检查是否有新数据（不等待 notify）
                // 这样空闲时写入能立即落盘，连续写入时能批量处理
                if wb.pending_len() > 0 {
                    continue;
                }
            }
        });

        // 保存句柄
        let handle_arc = self.flush_handle.clone();
        tokio::spawn(async move {
            *handle_arc.lock().await = Some(handle);
        });
    }

    /// 同步刷盘：等待所有 pending 操作落盘
    pub async fn flush(&self) -> Result<()> {
        // 通知后台任务立即刷盘
        loop {
            self.write_buffer.notify_flush();
            // 等待一小段时间让后台任务处理
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            if self.write_buffer.pending_len() == 0 {
                // 确保后台任务不在刷盘中
                if !self.write_buffer.flushing.load(Ordering::Acquire) {
                    break;
                }
            }
        }
        Ok(())
    }

    /// 写入对象（写缓存 + 提交到写缓冲区，后台批量刷盘）
    pub async fn put(
        &self,
        key: String,
        value: Bytes,
        content_type: Option<String>,
        tags: Option<serde_json::Value>,
    ) -> Result<ObjectMeta> {
        // 构造元数据
        let now = Utc::now();
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

        // 写入缓存（读路径优先命中缓存）
        self.cache.insert(key.clone(), value.clone());

        // 写入合并缓冲区（立即返回，后台批量刷盘）
        self.write_buffer.submit_put(key, value, meta.clone());

        Ok(meta)
    }

    /// 读取对象（缓存 → pending → 磁盘，自动回填缓存）
    pub async fn get(&self, key: &str) -> Result<(Bytes, ObjectMeta)> {
        // 先查缓存（DashMap/Moka 操作是轻量级的，可以留在 async 上下文）
        if let Some(cached) = self.cache.get(key) {
            match self.meta_store.get(key) {
                Ok(meta) => return Ok((cached.clone(), meta)),
                Err(_) => {
                    if let Some(op) = self.write_buffer.pending.get(key) {
                        if let PendingOp::Put { meta, .. } = op.value() {
                            return Ok((cached.clone(), meta.clone()));
                        }
                    }
                    self.flush().await?;
                    let meta = self.meta_store.get(key)?;
                    return Ok((cached.clone(), meta));
                }
            }
        }

        // 查 pending
        if let Some(op) = self.write_buffer.pending.get(key) {
            if let PendingOp::Put { value, meta } = op.value() {
                let value = value.clone();
                let meta = meta.clone();
                drop(op);
                self.cache.insert(key.to_string(), value.clone());
                return Ok((value, meta));
            }
        }

        // 查磁盘（用 spawn_blocking 包装同步 I/O）
        let kv_store = self.kv_store.clone();
        let meta_store = self.meta_store.clone();
        let key_owned = key.to_string();
        let (value, meta) = tokio::task::spawn_blocking(move || {
            let value = kv_store
                .get(&key_owned)?
                .ok_or_else(|| StoreError::KeyNotFound(key_owned.clone()))?;
            let meta = meta_store.get(&key_owned)?;
            Ok::<(Bytes, ObjectMeta), StoreError>((value, meta))
        })
        .await
        .map_err(|e| StoreError::InvalidArgument(format!("spawn_blocking 失败: {}", e)))??;

        self.cache.insert(key.to_string(), value.clone());

        Ok((value, meta))
    }

    /// 删除对象（清除缓存 + 提交删除到写缓冲区）
    pub async fn delete(&self, key: &str) -> Result<()> {
        // 删缓存
        self.cache.invalidate(key);
        // 提交删除到缓冲区（后台批量执行）
        self.write_buffer.submit_delete(key.to_string());
        Ok(())
    }

    /// 检查对象是否存在（pending → 缓存 → 磁盘）
    pub async fn exists(&self, key: &str) -> Result<bool> {
        // 先查 pending：如果是 Delete 则不存在，如果是 Put 则存在
        if let Some(op) = self.write_buffer.pending.get(key) {
            return Ok(matches!(op.value(), PendingOp::Put { .. }));
        }
        // 查缓存
        if self.cache.get(key).is_some() {
            return Ok(true);
        }
        // 查磁盘
        self.meta_store.exists(key)
    }

    /// 按前缀列出对象（磁盘 + pending 合并）
    pub async fn list(&self, prefix: &str, limit: usize) -> Result<Vec<ObjectMeta>> {
        // 先从磁盘获取基础列表
        let mut metas = self.meta_store.list(prefix, limit)?;
        // 合并 pending 中的更新（覆盖/新增/删除）
        let mut seen: std::collections::HashSet<String> =
            metas.iter().map(|m| m.key.clone()).collect();
        for entry in self.write_buffer.pending.iter() {
            let key = entry.key();
            if key.starts_with(prefix) {
                match entry.value() {
                    PendingOp::Put { meta, .. } => {
                        if !seen.contains(key) {
                            metas.push(meta.clone());
                            seen.insert(key.clone());
                        }
                    }
                    PendingOp::Delete => {
                        seen.remove(key);
                    }
                }
            }
        }
        // 过滤掉被删除的
        metas.retain(|m| seen.contains(&m.key));
        metas.sort_by(|a, b| a.key.cmp(&b.key));
        metas.truncate(limit);
        Ok(metas)
    }

    /// 批量写入对象
    pub async fn put_batch(
        &self,
        items: Vec<(String, Bytes, Option<String>, Option<serde_json::Value>)>,
    ) -> Result<Vec<ObjectMeta>> {
        let mut metas = Vec::with_capacity(items.len());
        let now = Utc::now();

        for (key, value, content_type, tags) in items {
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
            // 写缓存
            self.cache.insert(key.clone(), value.clone());
            // 写缓冲区
            self.write_buffer.submit_put(key, value, meta.clone());
            metas.push(meta);
        }

        Ok(metas)
    }

    /// 获取 KvStore 引用
    pub fn kv_store(&self) -> Arc<KvStore> {
        self.kv_store.clone()
    }

    /// 获取 MetaStore 引用
    pub fn meta_store(&self) -> Arc<MetaStore> {
        self.meta_store.clone()
    }
}

/// 执行批量刷盘：将 pending 操作一次性写入 jammdb + SQLite
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

    // 步骤 1：写 WAL
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
