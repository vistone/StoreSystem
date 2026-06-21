use crate::error::Result;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

// ============================================================
// 数据模型
// ============================================================

/// 对象元数据（与 KV 一一对应）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub content_type: Option<String>,
    pub tags: Option<serde_json::Value>,
    /// 数据校验和（SHA256），可选
    pub checksum: Option<String>,
    /// 存储节点标识（分布式场景），可选
    pub storage_node: Option<String>,
}

/// 按内容类型统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsByType {
    pub content_type: String,
    pub count: i64,
    pub total_size: i64,
    pub min_size: Option<i64>,
    pub max_size: Option<i64>,
    pub updated_at: DateTime<Utc>,
}

/// 按天统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsByDay {
    pub date: String,
    pub count: i64,
    pub total_size: i64,
    pub updated_at: DateTime<Utc>,
}

/// 按前缀/目录统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsByPrefix {
    pub prefix: String,
    pub count: i64,
    pub total_size: i64,
    pub updated_at: DateTime<Utc>,
}

/// 统计查询结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSummary {
    pub total_objects: i64,
    pub total_size: i64,
    pub by_type: Vec<StatsByType>,
    pub by_day: Vec<StatsByDay>,
    pub by_prefix: Vec<StatsByPrefix>,
}

/// WAL 记录（崩溃恢复时使用）
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub key: String,
    pub op_type: String, // "put" | "delete"
    pub meta_json: Option<String>,
}

// ============================================================
// MetaStore
// ============================================================

#[derive(Debug)]
pub struct MetaStore {
    conn: Mutex<Connection>,
}

impl MetaStore {
    /// 打开或创建 Meta 数据库
    ///
    /// # 参数
    /// - `path`: 数据库文件路径
    ///
    /// 自动创建所有必要的表和索引。
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;

        // SQLite 性能优化 PRAGMA
        let _: String = conn.query_row("PRAGMA journal_mode = WAL;", [], |row| row.get(0))?;
        conn.execute_batch(
            "PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -20000;
             PRAGMA temp_store = MEMORY;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;",
        )?;

        // ============================================================
        // 1. 对象明细表
        //    用途：精确查询、按条件过滤、列表展示
        //    KV 数据库只能按 key 精确查找或前缀扫描，
        //    而 SQLite 可以按 size/content_type/时间 等条件过滤
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS object_meta (
                key          TEXT PRIMARY KEY,
                size         INTEGER NOT NULL,
                content_type TEXT,
                created_at   TEXT NOT NULL,
                updated_at   TEXT NOT NULL,
                tags         JSON,
                checksum     TEXT,
                storage_node TEXT
            )",
            [],
        )?;

        // 明细表索引：加速按各种条件的查询
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_meta_size ON object_meta(size);",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_meta_content_type ON object_meta(content_type);",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_meta_created_at ON object_meta(created_at);",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_meta_updated_at ON object_meta(updated_at);",
            [],
        )?;

        // ============================================================
        // 2. 按内容类型统计表（聚合）
        //    用途：快速查看各类型数据的分布
        //    KV 数据库无法 GROUP BY content_type
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stats_by_type (
                content_type TEXT PRIMARY KEY,
                count        INTEGER NOT NULL DEFAULT 0,
                total_size   INTEGER NOT NULL DEFAULT 0,
                min_size     INTEGER,
                max_size     INTEGER,
                updated_at   TEXT NOT NULL
            )",
            [],
        )?;

        // ============================================================
        // 3. 按天统计表（聚合）
        //    用途：查看数据增长趋势
        //    KV 数据库无法按时间范围统计
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stats_by_day (
                date         TEXT PRIMARY KEY,
                count        INTEGER NOT NULL DEFAULT 0,
                total_size   INTEGER NOT NULL DEFAULT 0,
                updated_at   TEXT NOT NULL
            )",
            [],
        )?;

        // ============================================================
        // 4. 按前缀/目录统计表（聚合）
        //    用途：查看各目录/层级的存储分布
        //    KV 数据库的 scan(prefix) 需要遍历所有 key，效率低
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stats_by_prefix (
                prefix       TEXT PRIMARY KEY,
                count        INTEGER NOT NULL DEFAULT 0,
                total_size   INTEGER NOT NULL DEFAULT 0,
                updated_at   TEXT NOT NULL
            )",
            [],
        )?;

        // ============================================================
        // 5. 标签索引表
        //    用途：通过标签快速筛选对象
        //    KV 数据库无法按标签查询
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS tag_index (
                tag_key   TEXT NOT NULL,
                tag_value TEXT NOT NULL,
                obj_key   TEXT NOT NULL,
                PRIMARY KEY (tag_key, tag_value, obj_key)
            )",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tag_obj_key ON tag_index(obj_key);",
            [],
        )?;

        // ============================================================
        // 6. Write-Ahead Log（WAL）：保证 KV + Meta 写入的原子性
        //    写入流程：① 写入此表（意图记录）→ ② 写 jammdb KV → ③ 写 Meta + 删除此表记录
        //    崩溃恢复：启动时扫描此表，补写未完成的 Meta 记录
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS write_intent_wal (
                key        TEXT PRIMARY KEY,
                op_type    TEXT NOT NULL CHECK(op_type IN ('put', 'delete')),
                meta_json  TEXT,
                created_at TEXT NOT NULL
            )",
            [],
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    // ============================================================
    // WAL（Write-Ahead Log）操作
    // 用于保证 jammdb KV 写入 + SQLite Meta 写入的原子性
    // ============================================================

    /// WAL 条目（用于崩溃恢复）
    pub fn list_wal_entries(&self) -> Result<Vec<WalEntry>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let mut stmt = conn.prepare(
            "SELECT key, op_type, meta_json, created_at FROM write_intent_wal ORDER BY rowid",
        )?;
        let entries = stmt
            .query_map([], |row| {
                Ok(WalEntry {
                    key: row.get(0)?,
                    op_type: row.get(1)?,
                    meta_json: row.get(2)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(entries)
    }

    /// 批量写入 WAL 意图记录（在写 KV 之前调用）
    pub fn write_wal_batch(
        &self,
        puts: &[(String, String)],  // (key, meta_json)
        deletes: &[String],
    ) -> Result<()> {
        if puts.is_empty() && deletes.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let tx = conn.unchecked_transaction()?;
        let now = Utc::now().to_rfc3339();
        for (key, meta_json) in puts {
            tx.execute(
                "INSERT OR REPLACE INTO write_intent_wal (key, op_type, meta_json, created_at)
                 VALUES (?1, 'put', ?2, ?3)",
                params![key, meta_json, now],
            )?;
        }
        for key in deletes {
            tx.execute(
                "INSERT OR REPLACE INTO write_intent_wal (key, op_type, meta_json, created_at)
                 VALUES (?1, 'delete', NULL, ?2)",
                params![key, now],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    /// 提交 Meta 写入并原子地清除 WAL（在写 KV 之后调用）
    /// 一个事务内完成：写所有 Meta 记录 + 删除所有 WAL 条目
    pub fn commit_meta_clear_wal(
        &self,
        puts: &[ObjectMeta],
        del_keys: &[String],
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let tx = conn.unchecked_transaction()?;

        // 写入所有 put 的元数据
        for meta in puts {
            Self::put_in_tx(&tx, meta)?;
        }
        // 删除所有 delete 的元数据
        for key in del_keys {
            Self::delete_in_tx(&tx, key)?;
        }
        // 清除相关 WAL 条目（一次性）
        for meta in puts {
            tx.execute(
                "DELETE FROM write_intent_wal WHERE key = ?1",
                params![meta.key],
            )?;
        }
        for key in del_keys {
            tx.execute(
                "DELETE FROM write_intent_wal WHERE key = ?1",
                params![key],
            )?;
        }

        tx.commit()?;
        Ok(())
    }

    /// 删除单条 WAL 记录（崩溃恢复时使用）
    pub fn delete_wal_entry(&self, key: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        conn.execute(
            "DELETE FROM write_intent_wal WHERE key = ?1",
            params![key],
        )?;
        Ok(())
    }

    // ---- 内部事务辅助方法（供批量操作复用） ----

    fn put_in_tx(tx: &rusqlite::Transaction<'_>, meta: &ObjectMeta) -> Result<()> {
        let old_size: Option<i64> = tx
            .query_row(
                "SELECT size FROM object_meta WHERE key = ?1",
                params![meta.key],
                |row| row.get(0),
            )
            .ok();

        tx.execute(
            "INSERT OR REPLACE INTO object_meta
             (key, size, created_at, updated_at, content_type, tags, checksum, storage_node)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                meta.key,
                meta.size as i64,
                meta.created_at.to_rfc3339(),
                meta.updated_at.to_rfc3339(),
                meta.content_type,
                meta.tags,
                meta.checksum,
                meta.storage_node,
            ],
        )?;

        let now_str = Utc::now().to_rfc3339();
        let size_i64 = meta.size as i64;

        if let Some(ref ct) = meta.content_type {
            if let Some(old_s) = old_size {
                tx.execute(
                    "UPDATE stats_by_type SET
                        total_size = total_size - ?1 + ?2,
                        min_size = CASE WHEN min_size IS NULL THEN ?2
                                        WHEN ?2 < min_size THEN ?2 ELSE min_size END,
                        max_size = CASE WHEN max_size IS NULL THEN ?2
                                        WHEN ?2 > max_size THEN ?2 ELSE max_size END,
                        updated_at = ?3
                     WHERE content_type = ?4",
                    params![old_s, size_i64, now_str, ct],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO stats_by_type (content_type, count, total_size, min_size, max_size, updated_at)
                     VALUES (?1, 1, ?2, ?3, ?3, ?4)
                     ON CONFLICT(content_type) DO UPDATE SET
                        count = count + 1,
                        total_size = total_size + ?2,
                        min_size = CASE WHEN ?3 < min_size THEN ?3 ELSE min_size END,
                        max_size = CASE WHEN ?3 > max_size THEN ?3 ELSE max_size END,
                        updated_at = ?4",
                    params![ct, size_i64, size_i64, now_str],
                )?;
            }
        }

        let today = Utc::now().format("%Y-%m-%d").to_string();
        if old_size.is_some() {
            tx.execute(
                "UPDATE stats_by_day SET
                    total_size = total_size - ?1 + ?2,
                    updated_at = ?3
                 WHERE date = ?4",
                params![old_size.unwrap_or(0), size_i64, now_str, today],
            )?;
        } else {
            tx.execute(
                "INSERT INTO stats_by_day (date, count, total_size, updated_at)
                 VALUES (?1, 1, ?2, ?3)
                 ON CONFLICT(date) DO UPDATE SET
                    count = count + 1,
                    total_size = total_size + ?2,
                    updated_at = ?3",
                params![today, size_i64, now_str],
            )?;
        }

        if let Some(prefix) = extract_prefix(&meta.key) {
            if old_size.is_some() {
                tx.execute(
                    "UPDATE stats_by_prefix SET
                        total_size = total_size - ?1 + ?2,
                        updated_at = ?3
                     WHERE prefix = ?4",
                    params![old_size.unwrap_or(0), size_i64, now_str, prefix],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO stats_by_prefix (prefix, count, total_size, updated_at)
                     VALUES (?1, 1, ?2, ?3)
                     ON CONFLICT(prefix) DO UPDATE SET
                        count = count + 1,
                        total_size = total_size + ?2,
                        updated_at = ?3",
                    params![prefix, size_i64, now_str],
                )?;
            }
        }

        if let Some(ref tags) = meta.tags {
            if let Some(obj) = tags.as_object() {
                tx.execute(
                    "DELETE FROM tag_index WHERE obj_key = ?1",
                    params![meta.key],
                )?;
                for (k, v) in obj {
                    let val_str = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    tx.execute(
                        "INSERT OR IGNORE INTO tag_index (tag_key, tag_value, obj_key) VALUES (?1, ?2, ?3)",
                        params![k, val_str, meta.key],
                    )?;
                }
            }
        }
        Ok(())
    }

    fn delete_in_tx(tx: &rusqlite::Transaction<'_>, key: &str) -> Result<()> {
        let old: Option<(i64, Option<String>, String)> = tx
            .query_row(
                "SELECT size, content_type, created_at FROM object_meta WHERE key = ?1",
                params![key],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .ok();

        tx.execute("DELETE FROM object_meta WHERE key = ?1", params![key])?;
        tx.execute("DELETE FROM tag_index WHERE obj_key = ?1", params![key])?;

        if let Some((size, content_type, created_at_str)) = old {
            let now_str = Utc::now().to_rfc3339();
            if let Some(ref ct) = content_type {
                tx.execute(
                    "UPDATE stats_by_type SET count = count - 1, total_size = total_size - ?1, updated_at = ?2 WHERE content_type = ?3",
                    params![size, now_str, ct],
                )?;
            }
            if let Some(date) = created_at_str.split('T').next() {
                tx.execute(
                    "UPDATE stats_by_day SET count = count - 1, total_size = total_size - ?1, updated_at = ?2 WHERE date = ?3",
                    params![size, now_str, date],
                )?;
            }
            if let Some(prefix) = extract_prefix(key) {
                tx.execute(
                    "UPDATE stats_by_prefix SET count = count - 1, total_size = total_size - ?1, updated_at = ?2 WHERE prefix = ?3",
                    params![size, now_str, prefix],
                )?;
            }
        }
        Ok(())
    }

    // ============================================================
    // 对象明细操作
    // ============================================================

    /// 写入/更新对象元数据，同时更新聚合统计和标签索引
    ///
    /// 这是核心写入方法，一次调用完成：
    /// 1. 写入/更新 object_meta 明细
    /// 2. 更新 stats_by_type 聚合
    /// 3. 更新 stats_by_day 聚合
    /// 4. 更新 stats_by_prefix 聚合
    /// 5. 更新 tag_index
    ///
    /// 全部在同一个 SQLite 事务中完成，保证一致性。
    pub fn put(&self, meta: &ObjectMeta) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;

        let tx = conn.unchecked_transaction()?;

        // 获取旧记录（用于更新聚合统计时做差值）
        let old_size: Option<i64> = tx
            .query_row(
                "SELECT size FROM object_meta WHERE key = ?1",
                params![meta.key],
                |row| row.get(0),
            )
            .ok();

        // 1. 写入/更新明细
        tx.execute(
            "INSERT OR REPLACE INTO object_meta
             (key, size, created_at, updated_at, content_type, tags, checksum, storage_node)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                meta.key,
                meta.size as i64,
                meta.created_at.to_rfc3339(),
                meta.updated_at.to_rfc3339(),
                meta.content_type,
                meta.tags,
                meta.checksum,
                meta.storage_node,
            ],
        )?;

        let now_str = Utc::now().to_rfc3339();
        let size_i64 = meta.size as i64;

        // 2. 更新按类型统计
        if let Some(ref ct) = meta.content_type {
            if let Some(old_s) = old_size {
                // 更新已有记录：count 不变，total_size 做差值
                tx.execute(
                    "UPDATE stats_by_type SET
                        total_size = total_size - ?1 + ?2,
                        min_size = CASE WHEN min_size IS NULL THEN ?2
                                        WHEN ?2 < min_size THEN ?2 ELSE min_size END,
                        max_size = CASE WHEN max_size IS NULL THEN ?2
                                        WHEN ?2 > max_size THEN ?2 ELSE max_size END,
                        updated_at = ?3
                     WHERE content_type = ?4",
                    params![old_s, size_i64, now_str, ct],
                )?;
            } else {
                // 新记录
                tx.execute(
                    "INSERT INTO stats_by_type (content_type, count, total_size, min_size, max_size, updated_at)
                     VALUES (?1, 1, ?2, ?3, ?3, ?4)
                     ON CONFLICT(content_type) DO UPDATE SET
                        count = count + 1,
                        total_size = total_size + ?2,
                        min_size = CASE WHEN ?3 < min_size THEN ?3 ELSE min_size END,
                        max_size = CASE WHEN ?3 > max_size THEN ?3 ELSE max_size END,
                        updated_at = ?4",
                    params![ct, size_i64, size_i64, now_str],
                )?;
            }
        }

        // 3. 更新按天统计
        let today = Utc::now().format("%Y-%m-%d").to_string();
        if old_size.is_some() {
            tx.execute(
                "UPDATE stats_by_day SET
                    total_size = total_size - ?1 + ?2,
                    updated_at = ?3
                 WHERE date = ?4",
                params![old_size.unwrap_or(0), size_i64, now_str, today],
            )?;
        } else {
            tx.execute(
                "INSERT INTO stats_by_day (date, count, total_size, updated_at)
                 VALUES (?1, 1, ?2, ?3)
                 ON CONFLICT(date) DO UPDATE SET
                    count = count + 1,
                    total_size = total_size + ?2,
                    updated_at = ?3",
                params![today, size_i64, now_str],
            )?;
        }

        // 4. 更新按前缀统计
        if let Some(prefix) = extract_prefix(&meta.key) {
            if old_size.is_some() {
                tx.execute(
                    "UPDATE stats_by_prefix SET
                        total_size = total_size - ?1 + ?2,
                        updated_at = ?3
                     WHERE prefix = ?4",
                    params![old_size.unwrap_or(0), size_i64, now_str, prefix],
                )?;
            } else {
                tx.execute(
                    "INSERT INTO stats_by_prefix (prefix, count, total_size, updated_at)
                     VALUES (?1, 1, ?2, ?3)
                     ON CONFLICT(prefix) DO UPDATE SET
                        count = count + 1,
                        total_size = total_size + ?2,
                        updated_at = ?3",
                    params![prefix, size_i64, now_str],
                )?;
            }
        }

        // 5. 更新标签索引
        if let Some(ref tags) = meta.tags {
            if let Some(obj) = tags.as_object() {
                // 先删除旧标签
                tx.execute(
                    "DELETE FROM tag_index WHERE obj_key = ?1",
                    params![meta.key],
                )?;
                // 再插入新标签
                for (k, v) in obj {
                    let val_str = match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    tx.execute(
                        "INSERT OR IGNORE INTO tag_index (tag_key, tag_value, obj_key) VALUES (?1, ?2, ?3)",
                        params![k, val_str, meta.key],
                    )?;
                }
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// 获取对象元数据
    pub fn get(&self, key: &str) -> Result<ObjectMeta> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let mut stmt = conn.prepare(
            "SELECT key, size, created_at, updated_at, content_type, tags, checksum, storage_node
             FROM object_meta WHERE key = ?1",
        )?;

        let meta = stmt.query_row(params![key], |row| {
            let created_at_str: String = row.get(2)?;
            let updated_at_str: String = row.get(3)?;

            Ok(ObjectMeta {
                key: row.get(0)?,
                size: row.get::<_, i64>(1)? as u64,
                created_at: DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                content_type: row.get(4)?,
                tags: row.get(5)?,
                checksum: row.get(6)?,
                storage_node: row.get(7)?,
            })
        })?;

        Ok(meta)
    }

    /// 删除对象元数据，同时更新聚合统计和标签索引
    pub fn delete(&self, key: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;

        let tx = conn.unchecked_transaction()?;

        // 获取旧记录信息（用于更新聚合统计）
        let old: Option<(i64, Option<String>, String)> = tx
            .query_row(
                "SELECT size, content_type, created_at FROM object_meta WHERE key = ?1",
                params![key],
                |row| {
                    let size: i64 = row.get(0)?;
                    let ct: Option<String> = row.get(1)?;
                    let created: String = row.get(2)?;
                    Ok((size, ct, created))
                },
            )
            .ok();

        // 删除明细
        tx.execute("DELETE FROM object_meta WHERE key = ?1", params![key])?;

        // 删除标签索引
        tx.execute("DELETE FROM tag_index WHERE obj_key = ?1", params![key])?;

        if let Some((size, content_type, created_at_str)) = old {
            let now_str = Utc::now().to_rfc3339();

            // 更新按类型统计
            if let Some(ref ct) = content_type {
                tx.execute(
                    "UPDATE stats_by_type SET
                        count = count - 1,
                        total_size = total_size - ?1,
                        updated_at = ?2
                     WHERE content_type = ?3",
                    params![size, now_str, ct],
                )?;
            }

            // 更新按天统计
            if let Some(date) = created_at_str.split('T').next() {
                tx.execute(
                    "UPDATE stats_by_day SET
                        count = count - 1,
                        total_size = total_size - ?1,
                        updated_at = ?2
                     WHERE date = ?3",
                    params![size, now_str, date],
                )?;
            }

            // 更新按前缀统计
            if let Some(prefix) = extract_prefix(key) {
                tx.execute(
                    "UPDATE stats_by_prefix SET
                        count = count - 1,
                        total_size = total_size - ?1,
                        updated_at = ?2
                     WHERE prefix = ?3",
                    params![size, now_str, prefix],
                )?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// 判断对象是否存在
    pub fn exists(&self, key: &str) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let mut stmt = conn.prepare("SELECT 1 FROM object_meta WHERE key = ?1 LIMIT 1")?;
        let exists = stmt.exists(params![key])?;
        Ok(exists)
    }

    /// 按前缀列出对象元数据
    pub fn list(&self, prefix: &str, limit: usize) -> Result<Vec<ObjectMeta>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let mut stmt = conn.prepare(
            "SELECT key, size, created_at, updated_at, content_type, tags, checksum, storage_node
             FROM object_meta
             WHERE key LIKE ?1
             ORDER BY key
             LIMIT ?2",
        )?;

        let pattern = format!("{}%", prefix);
        let metas = stmt.query_map(params![pattern, limit as i64], |row| {
            let created_at_str: String = row.get(2)?;
            let updated_at_str: String = row.get(3)?;

            Ok(ObjectMeta {
                key: row.get(0)?,
                size: row.get::<_, i64>(1)? as u64,
                created_at: DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                content_type: row.get(4)?,
                tags: row.get(5)?,
                checksum: row.get(6)?,
                storage_node: row.get(7)?,
            })
        })?;

        let mut result = Vec::new();
        for meta in metas {
            result.push(meta?);
        }

        Ok(result)
    }

    // ============================================================
    // 批量操作
    // ============================================================

    /// 批量写入元数据（真正的单事务，比逐个 put 快 N 倍）
    pub fn put_batch_txn(&self, metas: &[ObjectMeta]) -> Result<()> {
        if metas.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let tx = conn.unchecked_transaction()?;
        for meta in metas {
            Self::put_in_tx(&tx, meta)?;
        }
        tx.commit()?;
        Ok(())
    }

    /// 批量删除元数据（真正的单事务）
    pub fn delete_batch_txn(&self, keys: &[String]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let tx = conn.unchecked_transaction()?;
        for key in keys {
            Self::delete_in_tx(&tx, key)?;
        }
        tx.commit()?;
        Ok(())
    }

    // ============================================================
    // 统计分析查询
    // ============================================================

    /// 获取完整统计摘要
    ///
    /// 返回：
    /// - 总对象数
    /// - 总大小
    /// - 按类型分布
    /// - 按天分布
    /// - 按前缀分布
    ///
    /// 这些查询在 KV 数据库中无法高效完成，
    /// 但在 SQLite 中通过聚合表可以极快返回。
    pub fn get_stats_summary(&self) -> Result<StatsSummary> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;

        // 总览
        let (total_objects, total_size): (i64, i64) = conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(size), 0) FROM object_meta",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        // 按类型统计
        let mut stmt = conn.prepare(
            "SELECT content_type, count, total_size, min_size, max_size, updated_at
             FROM stats_by_type WHERE count > 0 ORDER BY total_size DESC",
        )?;
        let by_type: Vec<StatsByType> = stmt
            .query_map([], |row| {
                let updated_at_str: String = row.get(5)?;
                Ok(StatsByType {
                    content_type: row.get(0)?,
                    count: row.get(1)?,
                    total_size: row.get(2)?,
                    min_size: row.get(3)?,
                    max_size: row.get(4)?,
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        // 按天统计
        let mut stmt = conn.prepare(
            "SELECT date, count, total_size, updated_at
             FROM stats_by_day WHERE count > 0 ORDER BY date DESC LIMIT 365",
        )?;
        let by_day: Vec<StatsByDay> = stmt
            .query_map([], |row| {
                let updated_at_str: String = row.get(3)?;
                Ok(StatsByDay {
                    date: row.get(0)?,
                    count: row.get(1)?,
                    total_size: row.get(2)?,
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        // 按前缀统计
        let mut stmt = conn.prepare(
            "SELECT prefix, count, total_size, updated_at
             FROM stats_by_prefix WHERE count > 0 ORDER BY total_size DESC LIMIT 100",
        )?;
        let by_prefix: Vec<StatsByPrefix> = stmt
            .query_map([], |row| {
                let updated_at_str: String = row.get(3)?;
                Ok(StatsByPrefix {
                    prefix: row.get(0)?,
                    count: row.get(1)?,
                    total_size: row.get(2)?,
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(StatsSummary {
            total_objects,
            total_size,
            by_type,
            by_day,
            by_prefix,
        })
    }

    /// 按内容类型查询统计
    pub fn get_stats_by_type(&self) -> Result<Vec<StatsByType>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let mut stmt = conn.prepare(
            "SELECT content_type, count, total_size, min_size, max_size, updated_at
             FROM stats_by_type WHERE count > 0 ORDER BY total_size DESC",
        )?;
        let results = stmt
            .query_map([], |row| {
                let updated_at_str: String = row.get(5)?;
                Ok(StatsByType {
                    content_type: row.get(0)?,
                    count: row.get(1)?,
                    total_size: row.get(2)?,
                    min_size: row.get(3)?,
                    max_size: row.get(4)?,
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(results)
    }

    /// 按天查询统计
    pub fn get_stats_by_day(&self, limit: usize) -> Result<Vec<StatsByDay>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let mut stmt = conn.prepare(
            "SELECT date, count, total_size, updated_at
             FROM stats_by_day WHERE count > 0 ORDER BY date DESC LIMIT ?1",
        )?;
        let results = stmt
            .query_map(params![limit as i64], |row| {
                let updated_at_str: String = row.get(3)?;
                Ok(StatsByDay {
                    date: row.get(0)?,
                    count: row.get(1)?,
                    total_size: row.get(2)?,
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(results)
    }

    /// 按前缀查询统计
    pub fn get_stats_by_prefix(
        &self,
        prefix_filter: Option<&str>,
        limit: usize,
    ) -> Result<Vec<StatsByPrefix>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;

        let (sql, param): (&str, Box<dyn rusqlite::types::ToSql>) = if let Some(pf) = prefix_filter
        {
            (
                "SELECT prefix, count, total_size, updated_at
              FROM stats_by_prefix WHERE prefix LIKE ?1 AND count > 0
              ORDER BY total_size DESC LIMIT ?2",
                Box::new(format!("{}%", pf)),
            )
        } else {
            (
                "SELECT prefix, count, total_size, updated_at
              FROM stats_by_prefix WHERE count > 0
              ORDER BY total_size DESC LIMIT ?1",
                Box::new(limit as i64),
            )
        };

        let mut stmt = conn.prepare(sql)?;
        let results = if prefix_filter.is_some() {
            stmt.query_map(params![param, limit as i64], |row| {
                let updated_at_str: String = row.get(3)?;
                Ok(StatsByPrefix {
                    prefix: row.get(0)?,
                    count: row.get(1)?,
                    total_size: row.get(2)?,
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![limit as i64], |row| {
                let updated_at_str: String = row.get(3)?;
                Ok(StatsByPrefix {
                    prefix: row.get(0)?,
                    count: row.get(1)?,
                    total_size: row.get(2)?,
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?
        };
        Ok(results)
    }

    /// 按标签查询对象
    ///
    /// 这是 KV 数据库完全无法支持的功能。
    /// 例如：查找所有 tag_key="user" AND tag_value="alice" 的对象
    pub fn query_by_tag(
        &self,
        tag_key: &str,
        tag_value: &str,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let mut stmt = conn.prepare(
            "SELECT o.key, o.size, o.created_at, o.updated_at, o.content_type, o.tags, o.checksum, o.storage_node
             FROM object_meta o
             JOIN tag_index t ON o.key = t.obj_key
             WHERE t.tag_key = ?1 AND t.tag_value = ?2
             ORDER BY o.key
             LIMIT ?3"
        )?;

        let metas = stmt.query_map(params![tag_key, tag_value, limit as i64], |row| {
            let created_at_str: String = row.get(2)?;
            let updated_at_str: String = row.get(3)?;

            Ok(ObjectMeta {
                key: row.get(0)?,
                size: row.get::<_, i64>(1)? as u64,
                created_at: DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                content_type: row.get(4)?,
                tags: row.get(5)?,
                checksum: row.get(6)?,
                storage_node: row.get(7)?,
            })
        })?;

        let mut result = Vec::new();
        for meta in metas {
            result.push(meta?);
        }
        Ok(result)
    }

    /// 按大小范围查询对象
    ///
    /// KV 数据库无法按大小过滤。
    /// 例如：查找所有大于 10MB 的对象
    pub fn query_by_size_range(
        &self,
        min_size: Option<u64>,
        max_size: Option<u64>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;

        let (sql, params_vec): (String, Vec<Box<dyn rusqlite::types::ToSql>>) = match (min_size, max_size) {
            (Some(min), Some(max)) => (
                "SELECT key, size, created_at, updated_at, content_type, tags, checksum, storage_node
                 FROM object_meta WHERE size >= ?1 AND size <= ?2
                 ORDER BY size DESC LIMIT ?3".to_string(),
                vec![Box::new(min as i64), Box::new(max as i64), Box::new(limit as i64)],
            ),
            (Some(min), None) => (
                "SELECT key, size, created_at, updated_at, content_type, tags, checksum, storage_node
                 FROM object_meta WHERE size >= ?1
                 ORDER BY size DESC LIMIT ?2".to_string(),
                vec![Box::new(min as i64), Box::new(limit as i64)],
            ),
            (None, Some(max)) => (
                "SELECT key, size, created_at, updated_at, content_type, tags, checksum, storage_node
                 FROM object_meta WHERE size <= ?1
                 ORDER BY size DESC LIMIT ?2".to_string(),
                vec![Box::new(max as i64), Box::new(limit as i64)],
            ),
            (None, None) => (
                "SELECT key, size, created_at, updated_at, content_type, tags, checksum, storage_node
                 FROM object_meta ORDER BY size DESC LIMIT ?1".to_string(),
                vec![Box::new(limit as i64)],
            ),
        };

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let metas = stmt
            .query_map(params_refs.as_slice(), |row| {
                let created_at_str: String = row.get(2)?;
                let updated_at_str: String = row.get(3)?;

                Ok(ObjectMeta {
                    key: row.get(0)?,
                    size: row.get::<_, i64>(1)? as u64,
                    created_at: DateTime::parse_from_rfc3339(&created_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                    content_type: row.get(4)?,
                    tags: row.get(5)?,
                    checksum: row.get(6)?,
                    storage_node: row.get(7)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(metas)
    }

    /// 按时间范围查询对象
    ///
    /// KV 数据库无法按时间过滤。
    /// 例如：查找 2026-06-01 之后创建的所有对象
    pub fn query_by_time_range(
        &self,
        start: &DateTime<Utc>,
        end: &DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<ObjectMeta>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MetaStore mutex poisoned".to_string())
        })?;
        let mut stmt = conn.prepare(
            "SELECT key, size, created_at, updated_at, content_type, tags, checksum, storage_node
             FROM object_meta
             WHERE created_at >= ?1 AND created_at <= ?2
             ORDER BY created_at DESC
             LIMIT ?3",
        )?;

        let metas = stmt.query_map(
            params![start.to_rfc3339(), end.to_rfc3339(), limit as i64],
            |row| {
                let created_at_str: String = row.get(2)?;
                let updated_at_str: String = row.get(3)?;

                Ok(ObjectMeta {
                    key: row.get(0)?,
                    size: row.get::<_, i64>(1)? as u64,
                    created_at: DateTime::parse_from_rfc3339(&created_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                2,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                    updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                        .map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                3,
                                rusqlite::types::Type::Text,
                                Box::new(e),
                            )
                        })?
                        .with_timezone(&Utc),
                    content_type: row.get(4)?,
                    tags: row.get(5)?,
                    checksum: row.get(6)?,
                    storage_node: row.get(7)?,
                })
            },
        )?;

        let mut result = Vec::new();
        for meta in metas {
            result.push(meta?);
        }
        Ok(result)
    }
}

// ============================================================
// 辅助函数
// ============================================================

/// 从 key 中提取前缀（第一个 / 之前的部分）
///
/// 例如：
/// - "15/123/456.png" → "15/"
/// - "images/photo.jpg" → "images/"
/// - "docs/report.pdf" → "docs/"
/// - "plain_key" → None
fn extract_prefix(key: &str) -> Option<String> {
    key.find('/').map(|pos| key[..=pos].to_string())
}
