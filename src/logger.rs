use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::{connect_async, WebSocketStream};
use tokio_tungstenite::tungstenite::Message;
use tokio::net::TcpStream;
use crate::error::Result;

// ============================================================
// 日志数据模型
// ============================================================

/// 日志级别
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// 调试信息
    Debug,
    /// 普通信息
    Info,
    /// 警告
    Warning,
    /// 错误
    Error,
    /// 严重错误
    Critical,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Warning => write!(f, "warning"),
            LogLevel::Error => write!(f, "error"),
            LogLevel::Critical => write!(f, "critical"),
        }
    }
}

impl From<&str> for LogLevel {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "debug" => LogLevel::Debug,
            "info" => LogLevel::Info,
            "warning" | "warn" => LogLevel::Warning,
            "error" => LogLevel::Error,
            "critical" | "fatal" => LogLevel::Critical,
            _ => LogLevel::Info,
        }
    }
}

/// 日志类别
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LogCategory {
    /// 系统健康（CPU/内存/磁盘）
    SystemHealth,
    /// 存储操作（Put/Get/Delete）
    Storage,
    /// 网络通信
    Network,
    /// 心跳
    Heartbeat,
    /// 注册/注销
    Registration,
    /// 安全事件
    Security,
    /// 用户自定义
    Custom(String),
}

impl std::fmt::Display for LogCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogCategory::SystemHealth => write!(f, "system_health"),
            LogCategory::Storage => write!(f, "storage"),
            LogCategory::Network => write!(f, "network"),
            LogCategory::Heartbeat => write!(f, "heartbeat"),
            LogCategory::Registration => write!(f, "registration"),
            LogCategory::Security => write!(f, "security"),
            LogCategory::Custom(s) => write!(f, "custom:{}", s),
        }
    }
}

impl From<&str> for LogCategory {
    fn from(s: &str) -> Self {
        match s {
            "system_health" => LogCategory::SystemHealth,
            "storage" => LogCategory::Storage,
            "network" => LogCategory::Network,
            "heartbeat" => LogCategory::Heartbeat,
            "registration" => LogCategory::Registration,
            "security" => LogCategory::Security,
            _ => LogCategory::Custom(s.to_string()),
        }
    }
}

/// 单条日志记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// 日志唯一 ID（自动生成）
    pub id: i64,
    /// Worker 节点 ID
    pub worker_id: String,
    /// 日志级别
    pub level: LogLevel,
    /// 日志类别
    pub category: LogCategory,
    /// 日志消息
    pub message: String,
    /// 详细数据（JSON 格式，可选）
    pub detail_json: Option<String>,
    /// 日志时间戳
    pub timestamp: DateTime<Utc>,
    /// 是否已读
    pub acknowledged: bool,
}

/// 日志查询过滤条件
#[derive(Debug, Clone, Default)]
pub struct LogQuery {
    pub worker_id: Option<String>,
    pub level: Option<LogLevel>,
    pub category: Option<LogCategory>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub keyword: Option<String>,
    pub unread_only: bool,
    pub limit: usize,
    pub offset: usize,
}

// ============================================================
// LogStore
// ============================================================

/// 日志存储（Master 端 SQLite）
///
/// 所有 Worker 的日志通过 WebSocket 实时推送到 Master，
/// Master 将日志持久化到 SQLite，并提供查询接口。
///
/// 日志量可能很大，因此：
/// - 按时间分区存储（按天分表）
/// - 自动清理过期日志（默认保留 30 天）
/// - 支持按级别/类别/Worker/时间范围过滤
#[derive(Debug, Clone)]
pub struct LogStore {
    conn: std::sync::Arc<Mutex<Connection>>,
    /// 日志保留天数
    retention_days: i64,
}

impl LogStore {
    /// 打开或创建日志数据库
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::open_with_retention(path, 30)
    }

    /// 打开或创建日志数据库（自定义保留天数）
    pub fn open_with_retention<P: AsRef<Path>>(path: P, retention_days: i64) -> Result<Self> {
        // 确保父目录存在
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = Connection::open(path)?;

        // SQLite 性能优化
        let _: String = conn.query_row("PRAGMA journal_mode = WAL;", [], |row| row.get(0))?;
        conn.execute_batch(
            "PRAGMA synchronous = NORMAL;
             PRAGMA cache_size = -16000;
             PRAGMA temp_store = MEMORY;
             PRAGMA busy_timeout = 5000;"
        )?;

        // ============================================================
        // 日志主表
        //    用途：存储所有 Worker 的日志记录
        //    索引策略：按时间、Worker、级别、类别建立复合索引
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS logs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id       TEXT NOT NULL,
                level           TEXT NOT NULL,
                category        TEXT NOT NULL,
                message         TEXT NOT NULL,
                detail_json     TEXT,
                timestamp       TEXT NOT NULL,
                acknowledged    INTEGER NOT NULL DEFAULT 0
            )",
            [],
        )?;

        // 查询索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_worker ON logs(worker_id, timestamp);", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level, timestamp);", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_category ON logs(category, timestamp);", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_unread ON logs(acknowledged, timestamp);", [])?;

        // ============================================================
        // 告警规则表
        //    用途：定义触发告警的条件
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alert_rules (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_name       TEXT NOT NULL UNIQUE,
                level           TEXT NOT NULL,
                category        TEXT,
                worker_id       TEXT,
                min_count       INTEGER NOT NULL DEFAULT 1,
                time_window_secs INTEGER NOT NULL DEFAULT 300,
                enabled         INTEGER NOT NULL DEFAULT 1,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL
            )",
            [],
        )?;

        // ============================================================
        // 告警记录表
        //    用途：记录触发的告警
        // ============================================================
        conn.execute(
            "CREATE TABLE IF NOT EXISTS alerts (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_id         INTEGER,
                rule_name       TEXT NOT NULL,
                worker_id       TEXT NOT NULL,
                level           TEXT NOT NULL,
                message         TEXT NOT NULL,
                count           INTEGER NOT NULL DEFAULT 1,
                first_occurrence TEXT NOT NULL,
                last_occurrence  TEXT NOT NULL,
                acknowledged    INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (rule_id) REFERENCES alert_rules(id)
            )",
            [],
        )?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_worker ON alerts(worker_id, last_occurrence);", [])?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_unread ON alerts(acknowledged, last_occurrence);", [])?;

        Ok(Self { conn: std::sync::Arc::new(Mutex::new(conn)), retention_days })
    }

    // ============================================================
    // 日志写入
    // ============================================================

    /// 写入单条日志
    pub fn write_log(&self, entry: &LogEntry) -> Result<i64> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        conn.execute(
            "INSERT INTO logs (worker_id, level, category, message, detail_json, timestamp, acknowledged)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                entry.worker_id,
                entry.level.to_string(),
                entry.category.to_string(),
                entry.message,
                entry.detail_json,
                entry.timestamp.to_rfc3339(),
                entry.acknowledged as i32,
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// 批量写入日志（高性能）
    pub fn write_logs_batch(&self, entries: &[LogEntry]) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let tx = conn.unchecked_transaction()?;

        let mut count = 0;
        for entry in entries {
            tx.execute(
                "INSERT INTO logs (worker_id, level, category, message, detail_json, timestamp, acknowledged)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    entry.worker_id,
                    entry.level.to_string(),
                    entry.category.to_string(),
                    entry.message,
                    entry.detail_json,
                    entry.timestamp.to_rfc3339(),
                    entry.acknowledged as i32,
                ],
            )?;
            count += 1;
        }

        tx.commit()?;
        Ok(count)
    }

    /// 快速写入日志（简化接口）
    pub fn log(
        &self,
        worker_id: &str,
        level: LogLevel,
        category: LogCategory,
        message: &str,
        detail: Option<&str>,
    ) -> Result<i64> {
        let entry = LogEntry {
            id: 0,
            worker_id: worker_id.to_string(),
            level,
            category,
            message: message.to_string(),
            detail_json: detail.map(|s| s.to_string()),
            timestamp: Utc::now(),
            acknowledged: false,
        };
        self.write_log(&entry)
    }

    // ============================================================
    // 日志查询
    // ============================================================

    /// 查询日志
    pub fn query_logs(&self, query: &LogQuery) -> Result<Vec<LogEntry>> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let mut sql = String::from(
            "SELECT id, worker_id, level, category, message, detail_json, timestamp, acknowledged FROM logs WHERE 1=1"
        );
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref worker_id) = query.worker_id {
            sql.push_str(" AND worker_id = ?");
            params_vec.push(Box::new(worker_id.clone()));
        }

        if let Some(ref level) = query.level {
            sql.push_str(" AND level = ?");
            params_vec.push(Box::new(level.to_string()));
        }

        if let Some(ref category) = query.category {
            sql.push_str(" AND category = ?");
            params_vec.push(Box::new(category.to_string()));
        }

        if let Some(ref start) = query.start_time {
            sql.push_str(" AND timestamp >= ?");
            params_vec.push(Box::new(start.to_rfc3339()));
        }

        if let Some(ref end) = query.end_time {
            sql.push_str(" AND timestamp <= ?");
            params_vec.push(Box::new(end.to_rfc3339()));
        }

        if let Some(ref keyword) = query.keyword {
            sql.push_str(" AND message LIKE ?");
            params_vec.push(Box::new(format!("%{}%", keyword)));
        }

        if query.unread_only {
            sql.push_str(" AND acknowledged = 0");
        }

        sql.push_str(" ORDER BY timestamp DESC LIMIT ? OFFSET ?");
        params_vec.push(Box::new(query.limit as i64));
        params_vec.push(Box::new(query.offset as i64));

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();

        let entries = stmt.query_map(params_refs.as_slice(), |row| {
            let timestamp_str: String = row.get(6)?;
            let level_str: String = row.get(2)?;
            let category_str: String = row.get(3)?;

            Ok(LogEntry {
                id: row.get(0)?,
                worker_id: row.get(1)?,
                level: LogLevel::from(level_str.as_str()),
                category: LogCategory::from(category_str.as_str()),
                message: row.get(4)?,
                detail_json: row.get(5)?,
                timestamp: DateTime::parse_from_rfc3339(&timestamp_str)
                    .map_err(|e| rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(e)))?
                    .with_timezone(&Utc),
                acknowledged: row.get::<_, i32>(7)? != 0,
            })
        })?;

        let mut result = Vec::new();
        for entry in entries {
            result.push(entry?);
        }
        Ok(result)
    }

    /// 获取日志总数（用于分页）
    pub fn count_logs(&self, query: &LogQuery) -> Result<usize> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let mut sql = String::from("SELECT COUNT(*) FROM logs WHERE 1=1");
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref worker_id) = query.worker_id {
            sql.push_str(" AND worker_id = ?");
            params_vec.push(Box::new(worker_id.clone()));
        }
        if let Some(ref level) = query.level {
            sql.push_str(" AND level = ?");
            params_vec.push(Box::new(level.to_string()));
        }
        if let Some(ref category) = query.category {
            sql.push_str(" AND category = ?");
            params_vec.push(Box::new(category.to_string()));
        }
        if let Some(ref start) = query.start_time {
            sql.push_str(" AND timestamp >= ?");
            params_vec.push(Box::new(start.to_rfc3339()));
        }
        if let Some(ref end) = query.end_time {
            sql.push_str(" AND timestamp <= ?");
            params_vec.push(Box::new(end.to_rfc3339()));
        }
        if let Some(ref keyword) = query.keyword {
            sql.push_str(" AND message LIKE ?");
            params_vec.push(Box::new(format!("%{}%", keyword)));
        }
        if query.unread_only {
            sql.push_str(" AND acknowledged = 0");
        }

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();
        let count: i64 = stmt.query_row(params_refs.as_slice(), |row| row.get(0))?;

        Ok(count as usize)
    }

    /// 获取最近的未读日志
    pub fn get_unread_logs(&self, limit: usize) -> Result<Vec<LogEntry>> {
        let mut query = LogQuery::default();
        query.unread_only = true;
        query.limit = limit;
        self.query_logs(&query)
    }

    /// 获取最近的错误/严重日志
    pub fn get_recent_errors(&self, limit: usize) -> Result<Vec<LogEntry>> {
        let mut query = LogQuery::default();
        query.limit = limit;
        // 查询 error 和 critical 级别的日志
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let mut stmt = conn.prepare(
            "SELECT id, worker_id, level, category, message, detail_json, timestamp, acknowledged
             FROM logs
             WHERE level IN ('error', 'critical')
             ORDER BY timestamp DESC
             LIMIT ?1"
        )?;

        let entries = stmt.query_map(params![limit as i64], |row| {
            let timestamp_str: String = row.get(6)?;
            let level_str: String = row.get(2)?;
            let category_str: String = row.get(3)?;

            Ok(LogEntry {
                id: row.get(0)?,
                worker_id: row.get(1)?,
                level: LogLevel::from(level_str.as_str()),
                category: LogCategory::from(category_str.as_str()),
                message: row.get(4)?,
                detail_json: row.get(5)?,
                timestamp: DateTime::parse_from_rfc3339(&timestamp_str)
                    .map_err(|e| rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(e)))?
                    .with_timezone(&Utc),
                acknowledged: row.get::<_, i32>(7)? != 0,
            })
        })?;

        let mut result = Vec::new();
        for entry in entries {
            result.push(entry?);
        }
        Ok(result)
    }

    // ============================================================
    // 日志管理
    // ============================================================

    /// 标记日志为已读
    pub fn acknowledge_log(&self, log_id: i64) -> Result<bool> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let rows = conn.execute(
            "UPDATE logs SET acknowledged = 1 WHERE id = ?1",
            params![log_id],
        )?;
        Ok(rows > 0)
    }

    /// 批量标记日志为已读
    pub fn acknowledge_logs_batch(&self, ids: &[i64]) -> Result<usize> {
        if ids.is_empty() {
            return Ok(0);
        }

        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let placeholders: Vec<String> = ids.iter().map(|_| "?".to_string()).collect();
        let sql = format!(
            "UPDATE logs SET acknowledged = 1 WHERE id IN ({})",
            placeholders.join(",")
        );

        let mut stmt = conn.prepare(&sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();
        let rows = stmt.execute(params_refs.as_slice())?;
        Ok(rows)
    }

    /// 标记某个 Worker 的所有日志为已读
    pub fn acknowledge_worker_logs(&self, worker_id: &str) -> Result<usize> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let rows = conn.execute(
            "UPDATE logs SET acknowledged = 1 WHERE worker_id = ?1 AND acknowledged = 0",
            params![worker_id],
        )?;
        Ok(rows)
    }

    /// 清理过期日志
    pub fn cleanup_old_logs(&self) -> Result<usize> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let cutoff = (Utc::now() - chrono::Duration::days(self.retention_days)).to_rfc3339();
        let rows = conn.execute("DELETE FROM logs WHERE timestamp < ?1", params![cutoff])?;
        Ok(rows)
    }

    /// 获取日志统计信息
    pub fn get_log_stats(&self) -> Result<LogStats> {
        let conn = self.conn.lock().map_err(|_| {
            crate::error::StoreError::InvalidArgument("LogStore mutex poisoned".to_string())
        })?;

        let total: i64 = conn.query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))?;
        let unread: i64 = conn.query_row("SELECT COUNT(*) FROM logs WHERE acknowledged = 0", [], |row| row.get(0))?;
        let errors: i64 = conn.query_row(
            "SELECT COUNT(*) FROM logs WHERE level IN ('error', 'critical')",
            [],
            |row| row.get(0),
        )?;
        let today: i64 = conn.query_row(
            "SELECT COUNT(*) FROM logs WHERE timestamp >= ?1",
            params![Utc::now().format("%Y-%m-%d").to_string()],
            |row| row.get(0),
        )?;

        // 获取各 Worker 的日志数量
        let mut stmt = conn.prepare(
            "SELECT worker_id, COUNT(*) as cnt FROM logs GROUP BY worker_id ORDER BY cnt DESC LIMIT 20"
        )?;
        let by_worker: Vec<(String, i64)> = stmt.query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?.collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(LogStats {
            total: total as usize,
            unread: unread as usize,
            errors: errors as usize,
            today: today as usize,
            by_worker,
        })
    }
}

/// 日志统计
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogStats {
    pub total: usize,
    pub unread: usize,
    pub errors: usize,
    pub today: usize,
    pub by_worker: Vec<(String, i64)>,
}

// ============================================================
// Worker 端日志采集器
// ============================================================

/// Worker 端日志采集器
///
/// 负责采集 Worker 的系统日志并通过 WebSocket 推送到 Master。
/// 使用持久 WebSocket 连接，避免每次推送都创建新连接。
pub struct WorkerLogger {
    /// Worker ID
    worker_id: String,
    /// Master 的 WebSocket 地址
    master_ws_addr: String,
    /// 日志缓冲区（批量推送）
    buffer: Mutex<Vec<LogEntry>>,
    /// 批量推送间隔（毫秒）
    flush_interval_ms: u64,
    /// 缓冲区最大大小
    max_buffer_size: usize,
    /// 持久 WebSocket 连接的发送端
    ws_tx: AsyncMutex<Option<tokio::sync::mpsc::UnboundedSender<Vec<LogEntry>>>>,
}

impl WorkerLogger {
    /// 创建 Worker 日志采集器
    pub fn new(worker_id: impl Into<String>, master_ws_addr: impl Into<String>) -> Self {
        Self {
            worker_id: worker_id.into(),
            master_ws_addr: master_ws_addr.into(),
            buffer: Mutex::new(Vec::with_capacity(100)),
            flush_interval_ms: 1000,  // 默认 1 秒推送一次
            max_buffer_size: 500,     // 缓冲区最大 500 条
            ws_tx: AsyncMutex::new(None),
        }
    }

    /// 设置推送间隔
    pub fn with_flush_interval(mut self, ms: u64) -> Self {
        self.flush_interval_ms = ms;
        self
    }

    /// 设置缓冲区大小
    pub fn with_max_buffer(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// 启动后台 WebSocket 连接维护任务
    /// 返回一个 JoinHandle，可以在程序退出时等待
    pub async fn start_background_connection(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                // 建立持久 WebSocket 连接
                match this.clone().connect_and_run().await {
                    Ok(_) => {
                        println!("[WorkerLogger] WS 连接正常关闭");
                    }
                    Err(e) => {
                        println!("[WorkerLogger] WS 连接异常: {}", e);
                    }
                }
                // 等待 3 秒后重连
                println!("[WorkerLogger] 3 秒后重连...");
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            }
        })
    }

    /// 建立连接并运行消息循环
    async fn connect_and_run(self: Arc<Self>) -> Result<()> {
        let ws_url = if self.master_ws_addr.starts_with("ws://") {
            self.master_ws_addr.clone()
        } else {
            format!("ws://{}", self.master_ws_addr)
        };

        println!("[WorkerLogger] 正在连接 WS: {}", ws_url);
        let (ws_stream, _) = connect_async(&ws_url)
            .await
            .map_err(|e| crate::error::StoreError::InvalidArgument(format!("WS 连接失败: {}", e)))?;
        println!("[WorkerLogger] WS 连接成功");

        let (mut write, mut read) = ws_stream.split();

        // 创建 channel 用于接收日志推送
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<LogEntry>>();

        // 保存发送端
        {
            let mut ws_tx = self.ws_tx.lock().await;
            *ws_tx = Some(tx);
        }

        // 发送初始连接消息（可选）
        let init_msg = serde_json::json!({
            "action": "log_batch",
            "payload": {
                "worker_id": self.worker_id,
                "entries": [],
            }
        });
        let _ = write.send(Message::Text(init_msg.to_string().into())).await;

        loop {
            tokio::select! {
                // 收到日志推送请求
                Some(entries) = rx.recv() => {
                    let payload = serde_json::json!({
                        "action": "log_batch",
                        "payload": {
                            "worker_id": self.worker_id,
                            "entries": entries.iter().map(|e| serde_json::json!({
                                "level": e.level.to_string(),
                                "category": e.category.to_string(),
                                "message": e.message,
                                "detail_json": e.detail_json,
                                "timestamp": e.timestamp.to_rfc3339(),
                            })).collect::<Vec<_>>(),
                        }
                    });

                    let text = serde_json::to_string(&payload)
                        .unwrap_or_default();

                    if write.send(Message::Text(text.into())).await.is_err() {
                        // 连接断开，将未发送的日志放回缓冲区
                        let mut buffer = self.buffer.lock().unwrap();
                        for entry in entries {
                            buffer.push(entry);
                        }
                        break;
                    }

                    // 读取确认响应
                    if let Some(Ok(Message::Text(response))) = read.next().await {
                        let resp: serde_json::Value = serde_json::from_str(&response).unwrap_or_default();
                        if resp["status"] != "ok" {
                            println!("[WorkerLogger] 日志推送被拒绝: {:?}", resp["message"]);
                        }
                    }
                }
                // 定期检查连接状态
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(30)) => {
                    // 发送心跳
                    let ping = serde_json::json!({
                        "action": "ping",
                        "payload": {
                            "worker_id": self.worker_id,
                        }
                    });
                    if write.send(Message::Text(ping.to_string().into())).await.is_err() {
                        break;
                    }
                }
            }
        }

        // 清理发送端
        {
            let mut ws_tx = self.ws_tx.lock().await;
            *ws_tx = None;
        }

        Ok(())
    }

    /// 记录日志（加入缓冲区）
    pub fn log(&self, level: LogLevel, category: LogCategory, message: &str, detail: Option<&str>) {
        let entry = LogEntry {
            id: 0,
            worker_id: self.worker_id.clone(),
            level,
            category,
            message: message.to_string(),
            detail_json: detail.map(|s| s.to_string()),
            timestamp: Utc::now(),
            acknowledged: false,
        };

        let mut buffer = self.buffer.lock().unwrap();
        buffer.push(entry);

        // 如果缓冲区满了，立即推送
        if buffer.len() >= self.max_buffer_size {
            drop(buffer);
            // 异步推送
            let logger_clone = self.clone();
            tokio::spawn(async move {
                let _ = logger_clone.flush().await;
            });
        }
    }

    /// 便捷方法：记录信息日志
    pub fn info(&self, category: LogCategory, message: &str) {
        self.log(LogLevel::Info, category, message, None);
    }

    /// 便捷方法：记录警告日志
    pub fn warn(&self, category: LogCategory, message: &str) {
        self.log(LogLevel::Warning, category, message, None);
    }

    /// 便捷方法：记录错误日志
    pub fn error(&self, category: LogCategory, message: &str, detail: Option<&str>) {
        self.log(LogLevel::Error, category, message, detail);
    }

    /// 便捷方法：记录严重错误日志
    pub fn critical(&self, category: LogCategory, message: &str, detail: Option<&str>) {
        self.log(LogLevel::Critical, category, message, detail);
    }

    /// 推送缓冲区中的日志到 Master
    pub async fn flush(&self) -> Result<usize> {
        let entries: Vec<LogEntry> = {
            let mut buffer = self.buffer.lock().unwrap();
            if buffer.is_empty() {
                return Ok(0);
            }
            std::mem::take(&mut *buffer)
        };

        // 通过持久 WebSocket 连接发送
        let ws_tx = self.ws_tx.lock().await;
        if let Some(ref tx) = *ws_tx {
            let len = entries.len();
            match tx.send(entries) {
                Ok(_) => Ok(len),
                Err(e) => {
                    // 发送失败，重新放回缓冲区
                    let mut buffer = self.buffer.lock().unwrap();
                    let remaining = e.0.len().min(self.max_buffer_size);
                    for entry in e.0.into_iter().take(remaining) {
                        buffer.push(entry);
                    }
                    println!("[WorkerLogger] 推送日志失败: channel 已关闭");
                    Ok(0)
                }
            }
        } else {
            // 连接未就绪，重新放回缓冲区
            let mut buffer = self.buffer.lock().unwrap();
            let remaining = entries.len().min(self.max_buffer_size);
            for entry in entries.into_iter().take(remaining) {
                buffer.push(entry);
            }
            Ok(0)
        }
    }
}

// WorkerLogger 需要 Clone 以便在异步任务中使用
impl Clone for WorkerLogger {
    fn clone(&self) -> Self {
        Self {
            worker_id: self.worker_id.clone(),
            master_ws_addr: self.master_ws_addr.clone(),
            buffer: Mutex::new(Vec::with_capacity(self.max_buffer_size)),
            flush_interval_ms: self.flush_interval_ms,
            max_buffer_size: self.max_buffer_size,
            ws_tx: AsyncMutex::new(None),
        }
    }
}
