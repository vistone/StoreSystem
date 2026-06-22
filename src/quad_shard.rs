use crate::config::QuadShardConfig;
use crate::error::{Result, StoreError};
use crate::kv::KvStore;
use crate::meta::{MetaStore, ObjectMeta};
use bytes::Bytes;
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// QuadKey 分片管理器
///
/// 根据 quadkey 的层级（level）自动路由数据到不同的 DB 文件：
/// - level ≤ base_level(默认8) → 所有数据存入 "base" DB
/// - base_level < level < split_level(默认18) → 按 quadkey 前 4 位分片
/// level ≥ split_level → 按 quadkey 前 8 位分片
#[derive(Debug)]
pub struct QuadShardManager {
    config: QuadShardConfig,
    /// 已打开的 shard 缓存: key = db_name
    shards: DashMap<String, Arc<QuadShard>>,
}

#[derive(Debug)]
struct QuadShard {
    kv_store: Arc<KvStore>,
    meta_store: Arc<MetaStore>,
}

impl QuadShardManager {
    /// 创建新的 QuadShardManager，自动创建数据根目录
    pub fn new(config: QuadShardConfig) -> Result<Self> {
        std::fs::create_dir_all(Path::new(&config.data_dir))?;
        Ok(Self {
            config,
            shards: DashMap::new(),
        })
    }

    /// 仅计算路径，不打开 DB（用于测试和日志）
    /// `epoch` 是版本号/桶名，由客户端传入
    pub fn route_paths(&self, epoch: &str, quadkey: &str, level: u32) -> (String, PathBuf, PathBuf) {
        let db_name = if level <= self.config.base_level {
            "base".to_string()
        } else if level < self.config.split_level {
            quadkey[..quadkey.len().min(4)].to_string()
        } else {
            quadkey[..quadkey.len().min(8)].to_string()
        };
        let ext = self.config.kv_ext.trim_start_matches('.');
        let meta_ext = self.config.meta_ext.trim_start_matches('.');
        let dir = if level <= self.config.base_level {
            PathBuf::from(&self.config.data_dir).join(epoch)
        } else {
            PathBuf::from(&self.config.data_dir)
                .join(epoch)
                .join(level.to_string())
        };
        let kv_path = dir.join(format!("{}.{}", db_name, ext));
        let meta_path = dir.join(format!("{}.{}", db_name, meta_ext));
        (db_name, kv_path, meta_path)
    }

    /// 根据 epoch + quadkey + level 确定目标 DB，懒加载打开
    fn route(&self, epoch: &str, quadkey: &str, level: u32) -> Result<Arc<QuadShard>> {
        let db_name = if level <= self.config.base_level {
            "base".to_string()
        } else if level < self.config.split_level {
            if quadkey.len() < 4 {
                return Err(StoreError::InvalidArgument(format!(
                    "quadkey 长度不足: level={}, len={}",
                    level,
                    quadkey.len()
                )));
            }
            quadkey[..4].to_string()
        } else {
            if quadkey.len() < 8 {
                return Err(StoreError::InvalidArgument(format!(
                    "quadkey 长度不足: level={}, len={}",
                    level,
                    quadkey.len()
                )));
            }
            quadkey[..8].to_string()
        };

        // 缓存查找 — 有就直接返回
        if let Some(shard) = self.shards.get(&db_name) {
            return Ok(shard.clone());
        }

        // lazy open — 首次访问时创建
        let ext = self.config.kv_ext.trim_start_matches('.');
        let meta_ext = self.config.meta_ext.trim_start_matches('.');
        let dir = if level <= self.config.base_level {
            PathBuf::from(&self.config.data_dir).join(epoch)
        } else {
            PathBuf::from(&self.config.data_dir)
                .join(epoch)
                .join(level.to_string())
        };
        std::fs::create_dir_all(&dir)?;
        let kv_path = dir.join(format!("{}.{}", db_name, ext));
        let meta_path = dir.join(format!("{}.{}", db_name, meta_ext));

        let kv_store = Arc::new(KvStore::open(&kv_path)?);
        let meta_store = Arc::new(MetaStore::open(&meta_path)?);
        let shard = Arc::new(QuadShard {
            kv_store,
            meta_store,
        });
        self.shards.insert(db_name, shard.clone());
        Ok(shard)
    }

    /// 写入对象
    pub fn put(
        &self,
        epoch: &str,
        quadkey: &str,
        level: u32,
        key: &str,
        value: Bytes,
        content_type: Option<String>,
        tags: Option<serde_json::Value>,
    ) -> Result<ObjectMeta> {
        let shard = self.route(epoch, quadkey, level)?;
        let now = chrono::Utc::now();
        let meta = ObjectMeta {
            key: key.to_string(),
            size: value.len() as u64,
            created_at: now,
            updated_at: now,
            content_type,
            tags,
            checksum: None,
            storage_node: None,
        };
        shard.kv_store.put(key, value)?;
        shard.meta_store.put(&meta)?;
        Ok(meta)
    }

    /// 读取对象
    pub fn get(&self, epoch: &str, quadkey: &str, level: u32, key: &str) -> Result<(Bytes, ObjectMeta)> {
        let shard = self.route(epoch, quadkey, level)?;
        let value = shard
            .kv_store
            .get(key)?
            .ok_or_else(|| StoreError::KeyNotFound(key.to_string()))?;
        let meta = shard.meta_store.get(key)?;
        Ok((value, meta))
    }

    /// 删除对象
    pub fn delete(&self, epoch: &str, quadkey: &str, level: u32, key: &str) -> Result<()> {
        let shard = self.route(epoch, quadkey, level)?;
        shard.kv_store.delete(key)?;
        shard.meta_store.delete(key)?;
        Ok(())
    }

    /// 检查对象是否存在
    pub fn exists(&self, epoch: &str, quadkey: &str, level: u32, key: &str) -> Result<bool> {
        let shard = self.route(epoch, quadkey, level)?;
        shard.meta_store.exists(key)
    }

    /// 按前缀列出对象元数据
    pub fn list(
        &self,
        epoch: &str,
        quadkey: &str,
        level: u32,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        let shard = self.route(epoch, quadkey, level)?;
        shard.meta_store.list(prefix, limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> QuadShardConfig {
        QuadShardConfig {
            base_level: 8,
            split_level: 18,
            data_dir: "/tmp/quad_test".to_string(),
            kv_ext: ".kv".to_string(),
            meta_ext: ".db".to_string(),
            cache_size: 100,
            flush_interval_ms: 5,
        }
    }

    #[test]
    fn test_route_level_leq_base() {
        let mgr = QuadShardManager::new(test_config()).unwrap();
        let (db_name, kv_path, _meta_path) = mgr.route_paths("v1", "30211", 5);
        assert_eq!(db_name, "base");
        assert!(kv_path.to_string_lossy().contains("v1/base.kv"));
        assert!(!kv_path.to_string_lossy().contains("/5/"));
    }

    #[test]
    fn test_route_level_mid() {
        let mgr = QuadShardManager::new(test_config()).unwrap();
        let (db_name, kv_path, _meta_path) = mgr.route_paths("v1", "302112345678", 12);
        assert_eq!(db_name, "3021");
        assert!(kv_path.to_string_lossy().contains("v1/12/3021.kv"));
    }

    #[test]
    fn test_route_level_high() {
        let mgr = QuadShardManager::new(test_config()).unwrap();
        let (db_name, kv_path, _meta_path) = mgr.route_paths("v1", "30211234567890123456", 20);
        assert_eq!(db_name, "30211234");
        assert!(kv_path.to_string_lossy().contains("v1/20/30211234.kv"));
    }

    #[test]
    fn test_put_and_get() {
        let config = test_config();
        let mgr = QuadShardManager::new(config).unwrap();
        let key = "test_key_1";
        let value = Bytes::from("hello quadkey");

        let meta = mgr
            .put("v1", "302112345678", 12, key, value.clone(), None, None)
            .unwrap();
        assert_eq!(meta.key, key);
        assert_eq!(meta.size, 13);

        let (read_val, read_meta) = mgr.get("v1", "302112345678", 12, key).unwrap();
        assert_eq!(read_val, value);
        assert_eq!(read_meta.key, key);

        // 清理测试数据
        let _ = std::fs::remove_dir_all("/tmp/quad_test");
    }

    #[test]
    fn test_put_base_level() {
        let config = test_config();
        let mgr = QuadShardManager::new(config).unwrap();
        mgr.put("v1", "30211", 5, "k1", Bytes::from("v1"), None, None)
            .unwrap();
        mgr.put("v1", "99999", 3, "k2", Bytes::from("v2"), None, None)
            .unwrap();
        // 两个不同 quadkey 但 level ≤ 8，应该在同一 base DB
        let (v1, _) = mgr.get("v1", "30211", 5, "k1").unwrap();
        let (v2, _) = mgr.get("v1", "99999", 3, "k2").unwrap();
        assert_eq!(v1, Bytes::from("v1"));
        assert_eq!(v2, Bytes::from("v2"));

        // 清理测试数据
        let _ = std::fs::remove_dir_all("/tmp/quad_test");
    }
}
