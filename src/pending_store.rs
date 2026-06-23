use crate::error::{Result, StoreError};
use bytes::Bytes;
use chrono::Utc;
use jammdb::DB;
use rusqlite::{params, Connection};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const KV_BUCKET: &str = "objects";

/// 单条 pending 条目
#[derive(Debug, Clone, PartialEq)]
pub struct PendingEntry {
    pub key: String,
    pub size: u64,
    pub created_at: String,
    pub status: String,
    pub attempt: u32,
}

/// 单个 region 的 pending 存储
struct RegionStore {
    kv: DB,
    meta: Connection,
}

/// Master 本地 Pending 缓存
///
/// 当区域 Worker 不可用时，Master 接管该区域的写入，存入 per-region jammdb + SQLite。
/// Worker 恢复后通过 WebSocket 拉取写回。
pub struct PendingStore {
    data_dir: PathBuf,
    regions: Mutex<HashMap<String, RegionStore>>,
}

impl PendingStore {
    /// 打开或创建 PendingStore
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            data_dir: dir,
            regions: Mutex::new(HashMap::new()),
        })
    }

    /// 获取或创建 region 的存储
    fn get_or_create_region(&self, region: &str) -> Result<()> {
        let mut regions = self.regions.lock().map_err(|_| {
            StoreError::InvalidArgument("PendingStore regions mutex poisoned".to_string())
        })?;
        if regions.contains_key(region) {
            return Ok(());
        }
        let kv_path = self.data_dir.join(format!("region_{}.kv", region));
        let meta_path = self.data_dir.join(format!("region_{}.meta", region));

        let kv = DB::open(&kv_path)?;
        {
            let tx = kv.tx(true)?;
            match tx.create_bucket(KV_BUCKET) {
                Ok(_) | Err(jammdb::Error::BucketExists) => {}
                Err(e) => return Err(e.into()),
            }
            tx.commit()?;
        }

        let meta = Connection::open(&meta_path)?;
        meta.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
        meta.execute(
            "CREATE TABLE IF NOT EXISTS pending_entries (
                key        TEXT PRIMARY KEY,
                size       INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                status     TEXT NOT NULL DEFAULT 'pending',
                attempt    INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )",
            [],
        )?;

        regions.insert(region.to_string(), RegionStore { kv, meta });
        Ok(())
    }

    /// 写入一条 pending 记录
    pub fn put(&self, region: &str, key: &str, value: &[u8]) -> Result<()> {
        self.get_or_create_region(region)?;
        let regions = self.regions.lock().map_err(|_| {
            StoreError::InvalidArgument("PendingStore regions mutex poisoned".to_string())
        })?;
        let store = regions.get(region).ok_or_else(|| {
            StoreError::InvalidArgument(format!("region {} not found", region))
        })?;

        let now = Utc::now().to_rfc3339();
        let size = value.len() as i64;

        // 写 KV
        {
            let tx = store.kv.tx(true)?;
            let bucket = tx.get_bucket(KV_BUCKET)?;
            bucket.put(key, value)?;
            tx.commit()?;
        }

        // 写 Meta（upsert）
        store.meta.execute(
            "INSERT INTO pending_entries (key, size, created_at, status, attempt, updated_at)
             VALUES (?1, ?2, ?3, 'pending', 0, ?4)
             ON CONFLICT(key) DO UPDATE SET
                size = ?2, status = 'pending', attempt = 0, updated_at = ?4",
            params![key, size, now, now],
        )?;

        Ok(())
    }

    /// 读取一条 pending 记录
    pub fn get(&self, region: &str, key: &str) -> Result<Option<Bytes>> {
        self.get_or_create_region(region)?;
        let regions = self.regions.lock().map_err(|_| {
            StoreError::InvalidArgument("PendingStore regions mutex poisoned".to_string())
        })?;
        let store = match regions.get(region) {
            Some(s) => s,
            None => return Ok(None),
        };

        let tx = store.kv.tx(false)?;
        let bucket = tx.get_bucket(KV_BUCKET)?;
        match bucket.get(key) {
            Some(data) if data.is_kv() => {
                Ok(Some(Bytes::copy_from_slice(data.kv().value())))
            }
            _ => Ok(None),
        }
    }

    /// 列出 region 中指定状态的条目
    pub fn list_by_status(&self, region: &str, statuses: &[&str]) -> Result<Vec<PendingEntry>> {
        self.get_or_create_region(region)?;
        let regions = self.regions.lock().map_err(|_| {
            StoreError::InvalidArgument("PendingStore regions mutex poisoned".to_string())
        })?;
        let store = match regions.get(region) {
            Some(s) => s,
            None => return Ok(Vec::new()),
        };

        let placeholders: Vec<String> = statuses.iter().enumerate().map(|(i, _)| format!("?{}", i + 1)).collect();
        let sql = format!(
            "SELECT key, size, created_at, status, attempt FROM pending_entries WHERE status IN ({})",
            placeholders.join(",")
        );
        let mut stmt = store.meta.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            statuses.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();
        let entries = stmt.query_map(params_refs.as_slice(), |row| {
            Ok(PendingEntry {
                key: row.get(0)?,
                size: row.get::<_, i64>(1)? as u64,
                created_at: row.get(2)?,
                status: row.get(3)?,
                attempt: row.get::<_, i64>(4)? as u32,
            })
        })?;

        let mut result = Vec::new();
        for entry in entries {
            result.push(entry?);
        }
        Ok(result)
    }

    /// 标记条目为 flushing
    pub fn mark_flushing(&self, region: &str, key: &str) -> Result<()> {
        self.update_status(region, key, "flushing")
    }

    /// 标记条目为 done
    pub fn mark_done(&self, region: &str, key: &str) -> Result<()> {
        self.update_status(region, key, "done")
    }

    /// 回退条目为 pending
    pub fn revert_to_pending(&self, region: &str, key: &str) -> Result<()> {
        self.update_status(region, key, "pending")
    }

    fn update_status(&self, region: &str, key: &str, status: &str) -> Result<()> {
        self.get_or_create_region(region)?;
        let regions = self.regions.lock().map_err(|_| {
            StoreError::InvalidArgument("PendingStore regions mutex poisoned".to_string())
        })?;
        let store = match regions.get(region) {
            Some(s) => s,
            None => return Ok(()),
        };

        let now = Utc::now().to_rfc3339();
        store.meta.execute(
            "UPDATE pending_entries SET status = ?1, updated_at = ?2 WHERE key = ?3",
            params![status, now, key],
        )?;
        Ok(())
    }

    /// GC: 清理 done 状态超过 retention_secs 的条目
    pub fn gc_done(&self, region: &str, retention_secs: u64) -> Result<usize> {
        self.get_or_create_region(region)?;
        let regions = self.regions.lock().map_err(|_| {
            StoreError::InvalidArgument("PendingStore regions mutex poisoned".to_string())
        })?;
        let store = match regions.get(region) {
            Some(s) => s,
            None => return Ok(0),
        };

        // 查询待删除的 key
        let cutoff = Utc::now()
            .checked_sub_signed(chrono::Duration::seconds(retention_secs as i64))
            .unwrap_or_else(|| Utc::now())
            .to_rfc3339();

        let mut stmt = store.meta.prepare(
            "SELECT key FROM pending_entries WHERE status = 'done' AND updated_at <= ?1",
        )?;
        let keys: Vec<String> = stmt
            .query_map(params![cutoff], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        let count = keys.len();
        if count == 0 {
            return Ok(0);
        }

        // 删除 Meta 记录
        {
            let tx = store.kv.tx(true)?;
            let bucket = tx.get_bucket(KV_BUCKET)?;
            for key in &keys {
                store.meta.execute(
                    "DELETE FROM pending_entries WHERE key = ?1",
                    params![key],
                )?;
                let _ = bucket.delete(key);
            }
            tx.commit()?;
        }

        Ok(count)
    }

    /// 将超时的 flushing 条目回退为 pending
    pub fn revert_stale_flushing(&self, region: &str, timeout_secs: u64) -> Result<usize> {
        self.get_or_create_region(region)?;
        let regions = self.regions.lock().map_err(|_| {
            StoreError::InvalidArgument("PendingStore regions mutex poisoned".to_string())
        })?;
        let store = match regions.get(region) {
            Some(s) => s,
            None => return Ok(0),
        };

        let cutoff = Utc::now()
            .checked_sub_signed(chrono::Duration::seconds(timeout_secs as i64))
            .unwrap_or_else(|| Utc::now())
            .to_rfc3339();

        let now = Utc::now().to_rfc3339();
        let count = store.meta.execute(
            "UPDATE pending_entries SET status = 'pending', attempt = attempt + 1, updated_at = ?1
             WHERE status = 'flushing' AND updated_at <= ?2",
            params![now, cutoff],
        )?;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (PendingStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = PendingStore::open(dir.path()).unwrap();
        (store, dir)
    }

    #[test]
    fn test_put_and_get() {
        let (store, _dir) = setup();
        store.put("2", "key_abc", b"hello world").unwrap();
        let val = store.get("2", "key_abc").unwrap();
        assert_eq!(val, Some(Bytes::from("hello world")));
    }

    #[test]
    fn test_get_nonexistent() {
        let (store, _dir) = setup();
        let val = store.get("2", "no_such_key").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_list_by_status() {
        let (store, _dir) = setup();
        store.put("2", "k1", b"v1").unwrap();
        store.put("2", "k2", b"v2").unwrap();
        store.put("2", "k3", b"v3").unwrap();

        let entries = store.list_by_status("2", &["pending"]).unwrap();
        assert_eq!(entries.len(), 3);
        assert!(entries.iter().all(|e| e.status == "pending"));
    }

    #[test]
    fn test_mark_flushing_and_done() {
        let (store, _dir) = setup();
        store.put("2", "k1", b"v1").unwrap();

        store.mark_flushing("2", "k1").unwrap();
        let entries = store.list_by_status("2", &["flushing"]).unwrap();
        assert_eq!(entries.len(), 1);

        store.mark_done("2", "k1").unwrap();
        let entries = store.list_by_status("2", &["done"]).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_revert_to_pending() {
        let (store, _dir) = setup();
        store.put("2", "k1", b"v1").unwrap();
        store.mark_flushing("2", "k1").unwrap();
        store.revert_to_pending("2", "k1").unwrap();

        let entries = store.list_by_status("2", &["pending"]).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn test_gc_done() {
        let (store, _dir) = setup();
        store.put("2", "k1", b"v1").unwrap();
        store.mark_done("2", "k1").unwrap();

        // Immediate GC with 0 retention should clean it
        let cleaned = store.gc_done("2", 0).unwrap();
        assert_eq!(cleaned, 1);

        let entries = store.list_by_status("2", &["done"]).unwrap();
        assert_eq!(entries.len(), 0);
        // KV should also be cleaned
        let val = store.get("2", "k1").unwrap();
        assert_eq!(val, None);
    }

    #[test]
    fn test_region_isolation() {
        let (store, _dir) = setup();
        store.put("0", "key", b"region0").unwrap();
        store.put("2", "key", b"region2").unwrap();

        assert_eq!(store.get("0", "key").unwrap(), Some(Bytes::from("region0")));
        assert_eq!(store.get("2", "key").unwrap(), Some(Bytes::from("region2")));
    }

    #[test]
    fn test_revert_stale_flushing() {
        let (store, _dir) = setup();
        store.put("2", "k1", b"v1").unwrap();
        store.mark_flushing("2", "k1").unwrap();

        // Revert with 0 timeout should immediately revert
        let reverted = store.revert_stale_flushing("2", 0).unwrap();
        assert_eq!(reverted, 1);

        let entries = store.list_by_status("2", &["pending"]).unwrap();
        assert_eq!(entries.len(), 1);
    }
}
