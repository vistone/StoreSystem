use crate::error::Result;
use bytes::Bytes;
use jammdb::DB;
use std::fmt;
use std::path::Path;
use std::sync::Mutex;

const BUCKET_NAME: &str = "objects";

/// 基于 bbolt (jammdb) 的嵌入式 KV 存储
/// B+树结构，ACID事务，内存映射文件，多读单写
pub struct KvStore {
    db: DB,
    write_lock: Mutex<()>,
}

impl fmt::Debug for KvStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KvStore")
            .field("db", &"jammdb::DB")
            .finish()
    }
}

impl KvStore {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = DB::open(path)?;
        // 初始化默认 bucket（若已存在则忽略）
        {
            let tx = db.tx(true)?;
            match tx.create_bucket(BUCKET_NAME) {
                Ok(_) => {}
                Err(jammdb::Error::BucketExists) => {}
                Err(e) => return Err(e.into()),
            }
            tx.commit()?;
        }
        Ok(Self {
            db,
            write_lock: Mutex::new(()),
        })
    }

    pub fn put(&self, key: &str, value: Bytes) -> Result<()> {
        let _guard = self.write_lock.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("KvStore write_lock poisoned".to_string())
        })?;
        let tx = self.db.tx(true)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        bucket.put(key, value.as_ref())?;
        tx.commit()?;
        Ok(())
    }

    pub fn put_batch(&self, kvs: Vec<(String, Bytes)>) -> Result<()> {
        let _guard = self.write_lock.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("KvStore write_lock poisoned".to_string())
        })?;
        let tx = self.db.tx(true)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        for (key, value) in &kvs {
            bucket.put(key.as_str(), value.as_ref())?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<Bytes>> {
        // 读事务无需加锁，jammdb 支持多读并发
        let tx = self.db.tx(false)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        match bucket.get(key) {
            Some(data) => {
                if data.is_kv() {
                    let kv = data.kv();
                    Ok(Some(Bytes::copy_from_slice(kv.value())))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    pub fn delete(&self, key: &str) -> Result<()> {
        let _guard = self.write_lock.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("KvStore write_lock poisoned".to_string())
        })?;
        let tx = self.db.tx(true)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        bucket.delete(key)?;
        tx.commit()?;
        Ok(())
    }

    pub fn delete_batch(&self, keys: Vec<String>) -> Result<()> {
        let _guard = self.write_lock.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("KvStore write_lock poisoned".to_string())
        })?;
        let tx = self.db.tx(true)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        for key in &keys {
            let _ = bucket.delete(key);
        }
        tx.commit()?;
        Ok(())
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        let tx = self.db.tx(false)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        Ok(bucket.get(key).is_some())
    }

    pub fn scan(&self, prefix: &str, limit: usize) -> Result<Vec<(String, Bytes)>> {
        let tx = self.db.tx(false)?;
        let bucket = tx.get_bucket(BUCKET_NAME)?;
        let cursor = bucket.cursor();

        let mut result = Vec::new();
        for item in cursor {
            if !item.is_kv() {
                continue;
            }
            let kv = item.kv();
            let key_bytes = kv.key();
            let key_str = String::from_utf8_lossy(key_bytes).to_string();
            if key_str.starts_with(prefix) {
                result.push((key_str, Bytes::copy_from_slice(kv.value())));
                if result.len() >= limit {
                    break;
                }
            }
        }
        Ok(result)
    }
}
