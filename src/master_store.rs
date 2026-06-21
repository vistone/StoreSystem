use crate::error::Result;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

// ============================================================
// Master 集群元数据模型
// ============================================================

/// Worker 节点注册信息（持久化到 SQLite）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub worker_id: String,
    pub address: String,
    pub weight: i32,
    pub tags_json: String, // JSON 格式的 tags
    pub region: String,    // Worker 负责的 quadkey 区域 (0/1/2/3)
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub alive: bool,
    // 健康监控
    pub storage_used_bytes: u64,
    pub storage_capacity_bytes: u64,
    pub storage_usage_ratio: f64,
    pub disk_health: String,
    pub memory_used_bytes: u64,
    pub memory_total_bytes: u64,
    pub memory_usage_ratio: f64,
    pub cpu_usage_ratio: f64,
    pub cpu_cores: u32,
    pub active_connections: u32,
    // 写入统计（v0.3.0 新增）
    pub total_put_count: u64,
    pub total_put_bytes: u64,
    pub flushed_count: u64,
    pub flushed_bytes: u64,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub write_rate_per_sec: f64,
    pub write_bytes_per_sec: f64,
}

/// 路由规则：key 前缀 -> Worker 映射
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteRule {
    pub key_prefix: String,
    pub worker_id: String,
    pub priority: i32,
    pub created_at: DateTime<Utc>,
}

/// 集群配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub key: String,
    pub value: String,
    pub updated_at: DateTime<Utc>,
}

/// 副本策略
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationPolicy {
    pub policy_name: String,
    pub replication_factor: i32,
    pub strategy: String, // "all" | "rack" | "custom"
    pub config_json: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ============================================================
// MasterStore
// ============================================================

/// Master 集群元数据存储
///
/// 使用 SQLite 持久化存储集群层面的元数据：
/// - Worker 节点注册信息（重启不丢失）
/// - 路由规则（key 前缀 -> Worker）
/// - 集群配置项
/// - 副本策略
///
/// 与 Worker 端的 MetaStore 不同，MasterStore 存储的是
/// 集群管理数据，而非对象元数据。
#[derive(Debug)]
pub struct MasterStore {
    conn: Mutex<Connection>,
}

impl MasterStore {
    /// 打开或创建 Master 元数据库
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        // 确保父目录存在
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(path)?;

        // SQLite 性能优化
        let _: String = conn.query_row("PRAGMA journal_mode = WAL;", [], |row| row.get(0))?;
        conn.execute_batch(
            "PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -8000;
             PRAGMA temp_store = MEMORY;
             PRAGMA busy_timeout = 5000;
             PRAGMA foreign_keys = ON;",
        )?;

        // ============================================================
        // 1. Worker 注册表
        //    用途：持久化存储 Worker 节点信息，Master 重启后不丢失
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS workers (
                worker_id          TEXT PRIMARY KEY,
                address            TEXT NOT NULL,
                weight             INTEGER NOT NULL DEFAULT 1,
                tags_json          TEXT NOT NULL DEFAULT '{}',
                registered_at      TEXT NOT NULL,
                last_heartbeat     TEXT NOT NULL,
                alive              INTEGER NOT NULL DEFAULT 1,
                storage_used_bytes    INTEGER NOT NULL DEFAULT 0,
                storage_capacity_bytes INTEGER NOT NULL DEFAULT 0,
                storage_usage_ratio   REAL NOT NULL DEFAULT 0.0,
                disk_health           TEXT NOT NULL DEFAULT 'Unknown',
                memory_used_bytes     INTEGER NOT NULL DEFAULT 0,
                memory_total_bytes    INTEGER NOT NULL DEFAULT 0,
                memory_usage_ratio    REAL NOT NULL DEFAULT 0.0,
                cpu_usage_ratio       REAL NOT NULL DEFAULT 0.0,
                cpu_cores             INTEGER NOT NULL DEFAULT 0,
                active_connections    INTEGER NOT NULL DEFAULT 0,
                total_put_count       INTEGER NOT NULL DEFAULT 0,
                total_put_bytes       INTEGER NOT NULL DEFAULT 0,
                flushed_count         INTEGER NOT NULL DEFAULT 0,
                flushed_bytes         INTEGER NOT NULL DEFAULT 0,
                pending_count         INTEGER NOT NULL DEFAULT 0,
                pending_bytes         INTEGER NOT NULL DEFAULT 0,
                write_rate_per_sec    REAL NOT NULL DEFAULT 0.0,
                write_bytes_per_sec   REAL NOT NULL DEFAULT 0.0,
                region               TEXT NOT NULL DEFAULT '0'
            )",
            [],
        )?;
        // 兼容旧库：尝试为新增列做 ALTER TABLE ADD COLUMN（已存在则忽略错误）
        let add_cols = [
            ("total_put_count", "INTEGER NOT NULL DEFAULT 0"),
            ("total_put_bytes", "INTEGER NOT NULL DEFAULT 0"),
            ("flushed_count", "INTEGER NOT NULL DEFAULT 0"),
            ("flushed_bytes", "INTEGER NOT NULL DEFAULT 0"),
            ("pending_count", "INTEGER NOT NULL DEFAULT 0"),
            ("pending_bytes", "INTEGER NOT NULL DEFAULT 0"),
            ("write_rate_per_sec", "REAL NOT NULL DEFAULT 0.0"),
            ("write_bytes_per_sec", "REAL NOT NULL DEFAULT 0.0"),
        ];
        for (col, decl) in add_cols.iter() {
            let sql = format!("ALTER TABLE workers ADD COLUMN {} {}", col, decl);
            let _ = conn.execute(&sql, []);
        }
        // 兼容旧库：region 列
        let _ = conn.execute(
            "ALTER TABLE workers ADD COLUMN region TEXT NOT NULL DEFAULT '0'",
            [],
        );
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_workers_alive ON workers(alive);",
            [],
        )?;

        // ============================================================
        // 2. 路由规则表
        //    用途：key 前缀到 Worker 的映射规则
        //    支持按前缀路由（如 "images/" -> worker-1, "docs/" -> worker-2）
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS route_rules (
                key_prefix   TEXT PRIMARY KEY,
                worker_id    TEXT NOT NULL,
                priority     INTEGER NOT NULL DEFAULT 0,
                created_at   TEXT NOT NULL,
                FOREIGN KEY (worker_id) REFERENCES workers(worker_id)
            )",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_route_worker ON route_rules(worker_id);",
            [],
        )?;

        // ============================================================
        // 3. 集群配置表
        //    用途：存储集群级别的配置项（键值对）
        //    如：replication_factor, storage_class, 等
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cluster_config (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )",
            [],
        )?;

        // ============================================================
        // 4. 副本策略表
        //    用途：定义数据的副本策略
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS replication_policies (
                policy_name       TEXT PRIMARY KEY,
                replication_factor INTEGER NOT NULL DEFAULT 1,
                strategy          TEXT NOT NULL DEFAULT 'all',
                config_json       TEXT NOT NULL DEFAULT '{}',
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            )",
            [],
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    // ============================================================
    // Worker 注册管理
    // ============================================================

    /// 注册 Worker（插入或更新）
    pub fn register_worker(
        &self,
        worker_id: &str,
        address: &str,
        weight: i32,
        tags: &HashMap<String, String>,
        region: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let now = Utc::now().to_rfc3339();
        let tags_json = serde_json::to_string(tags).unwrap_or_default();

        conn.execute(
            "INSERT INTO workers
             (worker_id, address, weight, tags_json, registered_at, last_heartbeat, alive, region)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7)
             ON CONFLICT(worker_id) DO UPDATE SET
                address = ?2,
                weight = ?3,
                tags_json = ?4,
                alive = 1,
                last_heartbeat = ?6,
                region = ?7",
            params![worker_id, address, weight, tags_json, now, now, region],
        )?;

        Ok(())
    }

    /// 更新 Worker 心跳
    #[allow(clippy::too_many_arguments)]
    pub fn update_heartbeat(
        &self,
        worker_id: &str,
        storage_used: u64,
        storage_capacity: u64,
        active_conns: u32,
        storage_usage_ratio: f64,
        disk_health: &str,
        memory_used: u64,
        memory_total: u64,
        memory_usage_ratio: f64,
        cpu_usage_ratio: f64,
        cpu_cores: u32,
        total_put_count: u64,
        total_put_bytes: u64,
        flushed_count: u64,
        flushed_bytes: u64,
        pending_count: u64,
        pending_bytes: u64,
        write_rate_per_sec: f64,
        write_bytes_per_sec: f64,
    ) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let now = Utc::now().to_rfc3339();

        let rows = conn.execute(
            "UPDATE workers SET
                alive = 1,
                last_heartbeat = ?1,
                storage_used_bytes = ?2,
                storage_capacity_bytes = ?3,
                active_connections = ?4,
                storage_usage_ratio = ?5,
                disk_health = ?6,
                memory_used_bytes = ?7,
                memory_total_bytes = ?8,
                memory_usage_ratio = ?9,
                cpu_usage_ratio = ?10,
                cpu_cores = ?11,
                total_put_count = ?12,
                total_put_bytes = ?13,
                flushed_count = ?14,
                flushed_bytes = ?15,
                pending_count = ?16,
                pending_bytes = ?17,
                write_rate_per_sec = ?18,
                write_bytes_per_sec = ?19
             WHERE worker_id = ?20",
            params![
                now,
                storage_used as i64,
                storage_capacity as i64,
                active_conns,
                storage_usage_ratio,
                disk_health,
                memory_used as i64,
                memory_total as i64,
                memory_usage_ratio,
                cpu_usage_ratio,
                cpu_cores,
                total_put_count as i64,
                total_put_bytes as i64,
                flushed_count as i64,
                flushed_bytes as i64,
                pending_count as i64,
                pending_bytes as i64,
                write_rate_per_sec,
                write_bytes_per_sec,
                worker_id,
            ],
        )?;

        Ok(rows > 0)
    }

    /// 获取所有 Worker 列表
    pub fn list_workers(&self, only_alive: bool) -> Result<Vec<WorkerRegistration>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let sql = if only_alive {
            "SELECT * FROM workers WHERE alive = 1 ORDER BY worker_id"
        } else {
            "SELECT * FROM workers ORDER BY worker_id"
        };

        let mut stmt = conn.prepare(sql)?;
        let workers = stmt.query_map([], |row| {
            let registered_at_str: String = row.get(4)?;
            let last_heartbeat_str: String = row.get(5)?;

            Ok(WorkerRegistration {
                worker_id: row.get(0)?,
                address: row.get(1)?,
                weight: row.get(2)?,
                tags_json: row.get(3)?,
                registered_at: DateTime::parse_from_rfc3339(&registered_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                last_heartbeat: DateTime::parse_from_rfc3339(&last_heartbeat_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                alive: row.get::<_, i32>(6)? != 0,
                storage_used_bytes: row.get::<_, i64>(7)? as u64,
                storage_capacity_bytes: row.get::<_, i64>(8)? as u64,
                storage_usage_ratio: row.get(9)?,
                disk_health: row.get(10)?,
                memory_used_bytes: row.get::<_, i64>(11)? as u64,
                memory_total_bytes: row.get::<_, i64>(12)? as u64,
                memory_usage_ratio: row.get(13)?,
                cpu_usage_ratio: row.get(14)?,
                cpu_cores: row.get::<_, i32>(15)? as u32,
                active_connections: row.get::<_, i32>(16)? as u32,
                total_put_count: row.get::<_, i64>(17)? as u64,
                total_put_bytes: row.get::<_, i64>(18)? as u64,
                flushed_count: row.get::<_, i64>(19)? as u64,
                flushed_bytes: row.get::<_, i64>(20)? as u64,
                pending_count: row.get::<_, i64>(21)? as u64,
                pending_bytes: row.get::<_, i64>(22)? as u64,
                write_rate_per_sec: row.get(23)?,
                write_bytes_per_sec: row.get(24)?,
                region: row.get(25)?,
            })
        })?;

        let mut result = Vec::new();
        for w in workers {
            result.push(w?);
        }
        Ok(result)
    }

    /// 获取单个 Worker 信息
    pub fn get_worker(&self, worker_id: &str) -> Result<Option<WorkerRegistration>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let mut stmt = conn.prepare("SELECT * FROM workers WHERE worker_id = ?1")?;

        let result = stmt.query_row(params![worker_id], |row| {
            let registered_at_str: String = row.get(4)?;
            let last_heartbeat_str: String = row.get(5)?;

            Ok(WorkerRegistration {
                worker_id: row.get(0)?,
                address: row.get(1)?,
                weight: row.get(2)?,
                tags_json: row.get(3)?,
                registered_at: DateTime::parse_from_rfc3339(&registered_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                last_heartbeat: DateTime::parse_from_rfc3339(&last_heartbeat_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
                alive: row.get::<_, i32>(6)? != 0,
                storage_used_bytes: row.get::<_, i64>(7)? as u64,
                storage_capacity_bytes: row.get::<_, i64>(8)? as u64,
                storage_usage_ratio: row.get(9)?,
                disk_health: row.get(10)?,
                memory_used_bytes: row.get::<_, i64>(11)? as u64,
                memory_total_bytes: row.get::<_, i64>(12)? as u64,
                memory_usage_ratio: row.get(13)?,
                cpu_usage_ratio: row.get(14)?,
                cpu_cores: row.get::<_, i32>(15)? as u32,
                active_connections: row.get::<_, i32>(16)? as u32,
                total_put_count: row.get::<_, i64>(17)? as u64,
                total_put_bytes: row.get::<_, i64>(18)? as u64,
                flushed_count: row.get::<_, i64>(19)? as u64,
                flushed_bytes: row.get::<_, i64>(20)? as u64,
                pending_count: row.get::<_, i64>(21)? as u64,
                pending_bytes: row.get::<_, i64>(22)? as u64,
                write_rate_per_sec: row.get(23)?,
                write_bytes_per_sec: row.get(24)?,
                region: row.get(25)?,
            })
        });

        match result {
            Ok(w) => Ok(Some(w)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 标记 Worker 为宕机
    pub fn mark_worker_dead(&self, worker_id: &str) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let rows = conn.execute(
            "UPDATE workers SET alive = 0 WHERE worker_id = ?1",
            params![worker_id],
        )?;

        Ok(rows > 0)
    }

    /// 清理超时的 Worker（标记为宕机）
    pub fn cleanup_dead_workers(&self, heartbeat_timeout_secs: u64) -> Result<usize> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let timeout = chrono::Duration::seconds(heartbeat_timeout_secs as i64);
        let cutoff = (Utc::now() - timeout).to_rfc3339();

        let rows = conn.execute(
            "UPDATE workers SET alive = 0
             WHERE alive = 1 AND last_heartbeat < ?1",
            params![cutoff],
        )?;

        Ok(rows)
    }

    /// 删除 Worker
    pub fn delete_worker(&self, worker_id: &str) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        // 先删除关联的路由规则
        conn.execute(
            "DELETE FROM route_rules WHERE worker_id = ?1",
            params![worker_id],
        )?;
        let rows = conn.execute(
            "DELETE FROM workers WHERE worker_id = ?1",
            params![worker_id],
        )?;

        Ok(rows > 0)
    }

    // ============================================================
    // 路由规则管理
    // ============================================================

    /// 添加或更新路由规则
    pub fn set_route_rule(&self, key_prefix: &str, worker_id: &str, priority: i32) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let now = Utc::now().to_rfc3339();

        conn.execute(
            "INSERT INTO route_rules (key_prefix, worker_id, priority, created_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(key_prefix) DO UPDATE SET
                worker_id = ?2,
                priority = ?3",
            params![key_prefix, worker_id, priority, now],
        )?;

        Ok(())
    }

    /// 删除路由规则
    pub fn delete_route_rule(&self, key_prefix: &str) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let rows = conn.execute(
            "DELETE FROM route_rules WHERE key_prefix = ?1",
            params![key_prefix],
        )?;
        Ok(rows > 0)
    }

    /// 获取所有路由规则
    pub fn list_route_rules(&self) -> Result<Vec<RouteRule>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let mut stmt = conn.prepare(
            "SELECT key_prefix, worker_id, priority, created_at
             FROM route_rules ORDER BY priority DESC, key_prefix",
        )?;

        let rules = stmt.query_map([], |row| {
            let created_at_str: String = row.get(3)?;
            Ok(RouteRule {
                key_prefix: row.get(0)?,
                worker_id: row.get(1)?,
                priority: row.get(2)?,
                created_at: DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
            })
        })?;

        let mut result = Vec::new();
        for r in rules {
            result.push(r?);
        }
        Ok(result)
    }

    /// 根据 key 查找匹配的路由规则（最长前缀匹配）
    pub fn find_route(&self, key: &str) -> Result<Option<RouteRule>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        // 查找所有可能匹配的前缀，按优先级和长度排序
        let mut stmt = conn.prepare(
            "SELECT key_prefix, worker_id, priority, created_at
             FROM route_rules
             WHERE ?1 LIKE key_prefix || '%'
             ORDER BY priority DESC, LENGTH(key_prefix) DESC
             LIMIT 1",
        )?;

        let result = stmt.query_row(params![key], |row| {
            let created_at_str: String = row.get(3)?;
            Ok(RouteRule {
                key_prefix: row.get(0)?,
                worker_id: row.get(1)?,
                priority: row.get(2)?,
                created_at: DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
            })
        });

        match result {
            Ok(rule) => Ok(Some(rule)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    // ============================================================
    // 集群配置管理
    // ============================================================

    /// 设置集群配置项
    pub fn set_config(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO cluster_config (key, value, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET value = ?2, updated_at = ?3",
            params![key, value, now],
        )?;

        Ok(())
    }

    /// 获取集群配置项
    pub fn get_config(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let mut stmt = conn.prepare("SELECT value FROM cluster_config WHERE key = ?1")?;
        let result = stmt.query_row(params![key], |row| row.get::<_, String>(0));

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 获取所有集群配置
    pub fn list_configs(&self) -> Result<Vec<ClusterConfig>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let mut stmt =
            conn.prepare("SELECT key, value, updated_at FROM cluster_config ORDER BY key")?;

        let configs = stmt.query_map([], |row| {
            let updated_at_str: String = row.get(2)?;
            Ok(ClusterConfig {
                key: row.get(0)?,
                value: row.get(1)?,
                updated_at: DateTime::parse_from_rfc3339(&updated_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            2,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
            })
        })?;

        let mut result = Vec::new();
        for c in configs {
            result.push(c?);
        }
        Ok(result)
    }

    /// 删除集群配置项
    pub fn delete_config(&self, key: &str) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let rows = conn.execute("DELETE FROM cluster_config WHERE key = ?1", params![key])?;
        Ok(rows > 0)
    }

    // ============================================================
    // 副本策略管理
    // ============================================================

    /// 设置副本策略
    pub fn set_replication_policy(
        &self,
        policy_name: &str,
        replication_factor: i32,
        strategy: &str,
        config_json: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO replication_policies (policy_name, replication_factor, strategy, config_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(policy_name) DO UPDATE SET
                replication_factor = ?2,
                strategy = ?3,
                config_json = ?4,
                updated_at = ?6",
            params![policy_name, replication_factor, strategy, config_json, now, now],
        )?;

        Ok(())
    }

    /// 获取所有副本策略
    pub fn list_replication_policies(&self) -> Result<Vec<ReplicationPolicy>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let mut stmt = conn.prepare(
            "SELECT policy_name, replication_factor, strategy, config_json, created_at, updated_at
             FROM replication_policies ORDER BY policy_name",
        )?;

        let policies = stmt.query_map([], |row| {
            let created_at_str: String = row.get(4)?;
            let updated_at_str: String = row.get(5)?;
            Ok(ReplicationPolicy {
                policy_name: row.get(0)?,
                replication_factor: row.get(1)?,
                strategy: row.get(2)?,
                config_json: row.get(3)?,
                created_at: DateTime::parse_from_rfc3339(&created_at_str)
                    .map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            4,
                            rusqlite::types::Type::Text,
                            Box::new(e),
                        )
                    })?
                    .with_timezone(&Utc),
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
        })?;

        let mut result = Vec::new();
        for p in policies {
            result.push(p?);
        }
        Ok(result)
    }

    /// 删除副本策略
    pub fn delete_replication_policy(&self, policy_name: &str) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("MasterStore mutex poisoned".to_string())
        })?;

        let rows = conn.execute(
            "DELETE FROM replication_policies WHERE policy_name = ?1",
            params![policy_name],
        )?;
        Ok(rows > 0)
    }
}
