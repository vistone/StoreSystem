//! Worker-side log collection and Master-side log persistence.
//!
//! # Overview
//! - [`LogLevel`] / [`LogCategory`]: classifiers for log entries.
//! - [`LogEntry`] / [`LogQuery`] / [`LogStats`]: public data types.
//! - [`LogStore`]: SQLite-backed log store used by the Master node.
//! - [`WorkerLogger`]: in-memory buffer + WebSocket sender used by Worker nodes.

use crate::error::{Result, StoreError};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio_tungstenite::tungstenite::Message;

// ============================================================
// LogLevel
// ============================================================

/// Severity level for a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

impl From<&str> for LogLevel {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "debug" => LogLevel::Debug,
            "warn" | "warning" => LogLevel::Warn,
            "error" => LogLevel::Error,
            _ => LogLevel::Info,
        }
    }
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "debug"),
            LogLevel::Info => write!(f, "info"),
            LogLevel::Warn => write!(f, "warn"),
            LogLevel::Error => write!(f, "error"),
        }
    }
}

// ============================================================
// LogCategory
// ============================================================

/// Functional category for a log entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogCategory {
    SystemHealth,
    DataOperation,
    Network,
    Storage,
    Custom(String),
}

impl From<&str> for LogCategory {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "system_health" | "systemhealth" => LogCategory::SystemHealth,
            "data_operation" | "dataoperation" => LogCategory::DataOperation,
            "network" => LogCategory::Network,
            "storage" => LogCategory::Storage,
            _ => LogCategory::Custom(s.to_string()),
        }
    }
}

impl std::fmt::Display for LogCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogCategory::SystemHealth => write!(f, "system_health"),
            LogCategory::DataOperation => write!(f, "data_operation"),
            LogCategory::Network => write!(f, "network"),
            LogCategory::Storage => write!(f, "storage"),
            LogCategory::Custom(s) => write!(f, "{}", s),
        }
    }
}

// ============================================================
// LogEntry
// ============================================================

/// A single log record (as stored / queried from [`LogStore`]).
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub id: i64,
    pub worker_id: String,
    pub level: LogLevel,
    pub category: LogCategory,
    pub message: String,
    pub detail_json: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub acknowledged: bool,
}

// ============================================================
// LogQuery
// ============================================================

/// Filter / pagination parameters for [`LogStore::query_logs`].
#[derive(Debug, Clone, Default)]
pub struct LogQuery {
    pub worker_id: Option<String>,
    pub level: Option<LogLevel>,
    pub category: Option<LogCategory>,
    pub keyword: Option<String>,
    pub unread_only: bool,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    /// Maximum number of rows to return (0 → default 100).
    pub limit: usize,
    pub offset: usize,
}

// ============================================================
// LogStats
// ============================================================

/// Aggregate statistics returned by [`LogStore::get_log_stats`].
#[derive(Debug, Clone)]
pub struct LogStats {
    pub total: i64,
    pub unread: i64,
    pub errors: i64,
    pub today: i64,
    pub by_worker: Vec<(String, i64)>,
}

// ============================================================
// LogStore
// ============================================================

/// SQLite-backed persistent log store used by the Master node.
///
/// Cheaply [`Clone`]-able (the underlying connection is wrapped in
/// `Arc<Mutex<…>>`).  All methods hold the mutex only for the duration
/// of the SQLite operation; they are **not** async and must not be
/// called while holding other locks that could cause priority inversion.
#[derive(Clone)]
pub struct LogStore {
    conn: Arc<Mutex<Connection>>,
}

impl LogStore {
    /// Open (or create) the log database at `path`.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;

        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA cache_size   = -4000;
             PRAGMA temp_store   = MEMORY;
             PRAGMA busy_timeout = 5000;",
        )?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS logs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                worker_id    TEXT    NOT NULL,
                level        TEXT    NOT NULL DEFAULT 'info',
                category     TEXT    NOT NULL DEFAULT 'custom',
                message      TEXT    NOT NULL,
                detail_json  TEXT,
                timestamp    TEXT    NOT NULL,
                acknowledged INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_logs_worker_id   ON logs (worker_id);
            CREATE INDEX IF NOT EXISTS idx_logs_level       ON logs (level);
            CREATE INDEX IF NOT EXISTS idx_logs_timestamp   ON logs (timestamp);
            CREATE INDEX IF NOT EXISTS idx_logs_acknowledged ON logs (acknowledged);",
        )?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    // ---- write helpers ----

    /// Append a single log entry and return its row id.
    pub fn log(
        &self,
        worker_id: &str,
        level: LogLevel,
        category: LogCategory,
        message: &str,
        detail_json: Option<&str>,
    ) -> Result<i64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StoreError::InvalidArgument("日志数据库锁定失败".to_string()))?;

        conn.execute(
            "INSERT INTO logs
               (worker_id, level, category, message, detail_json, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                worker_id,
                level.to_string(),
                category.to_string(),
                message,
                detail_json,
                Utc::now().to_rfc3339(),
            ],
        )?;

        Ok(conn.last_insert_rowid())
    }

    /// Append multiple entries in a single transaction.  Returns the number
    /// of rows inserted.
    pub fn write_logs_batch(&self, entries: &[LogEntry]) -> Result<i64> {
        if entries.is_empty() {
            return Ok(0);
        }

        let conn = self
            .conn
            .lock()
            .map_err(|_| StoreError::InvalidArgument("日志数据库锁定失败".to_string()))?;

        let tx = conn.unchecked_transaction()?;
        let mut count = 0i64;

        for entry in entries {
            tx.execute(
                "INSERT INTO logs
                   (worker_id, level, category, message, detail_json, timestamp)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    entry.worker_id,
                    entry.level.to_string(),
                    entry.category.to_string(),
                    entry.message,
                    entry.detail_json,
                    entry.timestamp.to_rfc3339(),
                ],
            )?;
            count += 1;
        }

        tx.commit()?;
        Ok(count)
    }

    // ---- read helpers ----

    /// Convert a raw SQLite row to a [`LogEntry`].
    fn row_to_entry(row: &rusqlite::Row<'_>) -> rusqlite::Result<LogEntry> {
        let level_str: String = row.get(2)?;
        let category_str: String = row.get(3)?;
        let timestamp_str: String = row.get(6)?;
        let ack_int: i32 = row.get(7)?;

        let timestamp = DateTime::parse_from_rfc3339(&timestamp_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());

        Ok(LogEntry {
            id: row.get(0)?,
            worker_id: row.get(1)?,
            level: LogLevel::from(level_str.as_str()),
            category: LogCategory::from(category_str.as_str()),
            message: row.get(4)?,
            detail_json: row.get(5)?,
            timestamp,
            acknowledged: ack_int != 0,
        })
    }

    /// Build a `WHERE …` clause string and a matching `Vec<String>` of
    /// bind values from a [`LogQuery`].  Values are bound as strings; the
    /// caller inserts `?1, ?2, …` placeholders via `params_from_iter`.
    fn build_where(query: &LogQuery) -> (String, Vec<String>) {
        let mut clauses: Vec<String> = Vec::new();
        let mut values: Vec<String> = Vec::new();

        if let Some(ref wid) = query.worker_id {
            values.push(wid.clone());
            clauses.push(format!("worker_id = ?{}", values.len()));
        }
        if let Some(ref level) = query.level {
            values.push(level.to_string());
            clauses.push(format!("level = ?{}", values.len()));
        }
        if let Some(ref cat) = query.category {
            values.push(cat.to_string());
            clauses.push(format!("category = ?{}", values.len()));
        }
        if let Some(ref kw) = query.keyword {
            values.push(format!("%{}%", kw));
            clauses.push(format!("message LIKE ?{}", values.len()));
        }
        if query.unread_only {
            clauses.push("acknowledged = 0".to_string());
        }
        if let Some(start) = query.start_time {
            values.push(start.to_rfc3339());
            clauses.push(format!("timestamp >= ?{}", values.len()));
        }
        if let Some(end) = query.end_time {
            values.push(end.to_rfc3339());
            clauses.push(format!("timestamp <= ?{}", values.len()));
        }

        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };

        (where_sql, values)
    }

    /// Query log entries with optional filtering and pagination.
    pub fn query_logs(&self, query: &LogQuery) -> Result<Vec<LogEntry>> {
        let (where_sql, values) = Self::build_where(query);
        let limit = if query.limit == 0 { 100 } else { query.limit };

        let sql = format!(
            "SELECT id, worker_id, level, category, message, detail_json, timestamp, acknowledged
             FROM logs{}
             ORDER BY timestamp DESC
             LIMIT {} OFFSET {}",
            where_sql, limit, query.offset
        );

        let conn = self
            .conn
            .lock()
            .map_err(|_| StoreError::InvalidArgument("日志数据库锁定失败".to_string()))?;

        let mut stmt = conn.prepare(&sql)?;
        let entries = stmt
            .query_map(
                rusqlite::params_from_iter(values.iter()),
                Self::row_to_entry,
            )?
            .filter_map(|r| r.ok())
            .collect();

        Ok(entries)
    }

    /// Count matching log entries (uses the same filter as [`query_logs`]).
    pub fn count_logs(&self, query: &LogQuery) -> Result<i64> {
        let (where_sql, values) = Self::build_where(query);
        let sql = format!("SELECT COUNT(*) FROM logs{}", where_sql);

        let conn = self
            .conn
            .lock()
            .map_err(|_| StoreError::InvalidArgument("日志数据库锁定失败".to_string()))?;

        let count: i64 = conn.query_row(&sql, rusqlite::params_from_iter(values.iter()), |r| {
            r.get::<_, i64>(0)
        })?;

        Ok(count)
    }

    /// Return aggregate statistics.
    pub fn get_log_stats(&self) -> Result<LogStats> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StoreError::InvalidArgument("日志数据库锁定失败".to_string()))?;

        let total: i64 = conn.query_row("SELECT COUNT(*) FROM logs", [], |r| r.get::<_, i64>(0))?;

        let unread: i64 = conn.query_row(
            "SELECT COUNT(*) FROM logs WHERE acknowledged = 0",
            [],
            |r| r.get::<_, i64>(0),
        )?;

        let errors: i64 =
            conn.query_row("SELECT COUNT(*) FROM logs WHERE level = 'error'", [], |r| {
                r.get::<_, i64>(0)
            })?;

        // Today's entries (UTC midnight as cutoff)
        let today_start = Utc::now()
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
            .unwrap_or_else(Utc::now);

        let today: i64 = conn.query_row(
            "SELECT COUNT(*) FROM logs WHERE timestamp >= ?1",
            params![today_start.to_rfc3339()],
            |r| r.get::<_, i64>(0),
        )?;

        let mut stmt = conn.prepare(
            "SELECT worker_id, COUNT(*) AS cnt FROM logs
             GROUP BY worker_id ORDER BY cnt DESC",
        )?;
        let by_worker: Vec<(String, i64)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(LogStats {
            total,
            unread,
            errors,
            today,
            by_worker,
        })
    }

    /// Fetch the `limit` most recent error entries.
    pub fn get_recent_errors(&self, limit: usize) -> Result<Vec<LogEntry>> {
        self.query_logs(&LogQuery {
            level: Some(LogLevel::Error),
            limit,
            ..Default::default()
        })
    }

    /// Fetch unread entries (up to `limit`).
    pub fn get_unread_logs(&self, limit: usize) -> Result<Vec<LogEntry>> {
        self.query_logs(&LogQuery {
            unread_only: true,
            limit,
            ..Default::default()
        })
    }

    /// Mark a single entry as acknowledged.  Returns `true` if the row
    /// existed.
    pub fn acknowledge_log(&self, id: i64) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| StoreError::InvalidArgument("日志数据库锁定失败".to_string()))?;

        let affected = conn.execute(
            "UPDATE logs SET acknowledged = 1 WHERE id = ?1",
            params![id],
        )?;

        Ok(affected > 0)
    }

    /// Mark a batch of entries as acknowledged.  Returns the number of rows
    /// updated.
    pub fn acknowledge_logs_batch(&self, ids: &[i64]) -> Result<i64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let conn = self
            .conn
            .lock()
            .map_err(|_| StoreError::InvalidArgument("日志数据库锁定失败".to_string()))?;

        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{}", i)).collect();
        let sql = format!(
            "UPDATE logs SET acknowledged = 1 WHERE id IN ({})",
            placeholders.join(", ")
        );

        let mut stmt = conn.prepare(&sql)?;
        let affected = stmt.execute(rusqlite::params_from_iter(ids))?;

        Ok(affected as i64)
    }
}

// ============================================================
// WorkerLogger
// ============================================================

/// Internal buffer entry (cheaper than the public [`LogEntry`]).
#[derive(Debug, Clone)]
struct LocalLogEntry {
    level: LogLevel,
    category: LogCategory,
    message: String,
    detail_json: Option<String>,
    timestamp: DateTime<Utc>,
}

/// The type of the WebSocket write-half used by [`WorkerLogger`].
type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Message,
>;

/// Worker-side log collector.
///
/// Buffers log entries in memory and forwards them to the Master via a
/// persistent WebSocket connection.
///
/// # Guarantees
/// - `flush` **never panics**.
/// - When `flush` cannot deliver entries (WS not connected, send error, or
///   serialization failure) the entries are **restored to the buffer** so
///   they can be retried in the next flush cycle.
/// - The buffer is capped at `max_buffer_size`; entries exceeding the cap
///   are dropped (oldest-first when restoring after a failed flush).
///
/// # Usage
/// ```text
/// let logger = Arc::new(WorkerLogger::new("worker-1", "127.0.0.1:50053")
///     .with_flush_interval(1000)
///     .with_max_buffer(500));
///
/// // Spawn background connection maintenance
/// let bg = logger.clone();
/// tokio::spawn(async move { bg.start_background_connection().await; });
///
/// // Spawn periodic flush
/// let fl = logger.clone();
/// tokio::spawn(async move {
///     loop {
///         tokio::time::sleep(Duration::from_millis(1000)).await;
///         let _ = fl.flush().await;
///     }
/// });
///
/// logger.info(LogCategory::SystemHealth, "Worker started");
/// ```
pub struct WorkerLogger {
    worker_id: String,
    master_ws_addr: String,
    /// In-memory buffer.  Hold the lock only for the minimum time.
    buffer: Mutex<Vec<LocalLogEntry>>,
    max_buffer_size: usize,
    #[allow(dead_code)]
    flush_interval_ms: u64,
    /// Shared WebSocket write-half.  `None` when not connected.
    ws_sink: tokio::sync::Mutex<Option<WsSink>>,
    /// 配置更新回调（Master 推送 config_update 消息时触发）
    /// 使用 std::sync::Mutex 因为只在构造时写一次，之后只读
    #[allow(clippy::type_complexity)]
    config_update_handler:
        std::sync::Arc<std::sync::Mutex<Option<Box<dyn Fn(String) + Send + Sync>>>>,
}

impl WorkerLogger {
    /// Create a new logger that will push to `master_ws_addr`.
    ///
    /// The address may optionally include a `ws://` prefix; it will be
    /// stripped.
    pub fn new(worker_id: &str, master_ws_addr: &str) -> Self {
        let addr = master_ws_addr
            .trim_start_matches("ws://")
            .trim_start_matches("wss://");

        Self {
            worker_id: worker_id.to_string(),
            master_ws_addr: addr.to_string(),
            buffer: Mutex::new(Vec::new()),
            max_buffer_size: 1000,
            flush_interval_ms: 1000,
            ws_sink: tokio::sync::Mutex::new(None),
            config_update_handler: std::sync::Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Override the flush interval hint (milliseconds).
    pub fn with_flush_interval(mut self, ms: u64) -> Self {
        self.flush_interval_ms = ms;
        self
    }

    /// Override the maximum buffer size (entries).
    pub fn with_max_buffer(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// 设置配置更新回调（Master 推送 config_update 消息时触发）
    ///
    /// 回调参数为配置 JSON 字符串。
    pub fn with_config_update_handler<F>(self, handler: F) -> Self
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        if let Ok(mut guard) = self.config_update_handler.lock() {
            *guard = Some(Box::new(handler));
        } else {
            eprintln!("[WorkerLogger] 设置 config_update_handler 失败: Mutex 已中毒");
        }
        self
    }

    // ---- log helpers ----

    fn push(&self, entry: LocalLogEntry) {
        if let Ok(mut buf) = self.buffer.lock() {
            if buf.len() < self.max_buffer_size {
                buf.push(entry);
            }
        }
    }

    /// Log with an explicit level, category, optional detail JSON.
    pub fn log_with_detail(
        &self,
        level: LogLevel,
        category: LogCategory,
        message: &str,
        detail_json: Option<&str>,
    ) {
        self.push(LocalLogEntry {
            level,
            category,
            message: message.to_string(),
            detail_json: detail_json.map(|s| s.to_string()),
            timestamp: Utc::now(),
        });
    }

    /// Log at `Info` level.
    pub fn info(&self, category: LogCategory, message: &str) {
        self.log_with_detail(LogLevel::Info, category, message, None);
    }

    /// Log at `Warn` level.
    pub fn warn(&self, category: LogCategory, message: &str) {
        self.log_with_detail(LogLevel::Warn, category, message, None);
    }

    /// Log at `Error` level.
    pub fn error(&self, category: LogCategory, message: &str) {
        self.log_with_detail(LogLevel::Error, category, message, None);
    }

    /// Log at `Debug` level.
    pub fn debug(&self, category: LogCategory, message: &str) {
        self.log_with_detail(LogLevel::Debug, category, message, None);
    }

    /// Prepend `entries` back to the buffer, respecting `max_buffer_size`.
    ///
    /// Older (returned) entries are placed before newer buffered entries so
    /// the overall ordering is preserved as much as possible.
    fn restore_to_buffer(&self, entries: Vec<LocalLogEntry>) {
        if entries.is_empty() {
            return;
        }

        if let Ok(mut buf) = self.buffer.lock() {
            let available = self.max_buffer_size.saturating_sub(buf.len());
            let take = entries.len().min(available);

            // Build: [returned (up to `take`)] ++ [existing buffer]
            let mut restored: Vec<LocalLogEntry> = entries.into_iter().take(take).collect();
            restored.append(&mut *buf);
            *buf = restored;
        }
    }

    // ---- flush ----

    /// Drain the in-memory buffer and send all entries to Master.
    ///
    /// On any failure the unsent entries are returned to the buffer.
    /// Returns `Ok(())` when all entries were delivered successfully (or when
    /// there was nothing to send), and `Err(_)` otherwise.
    pub async fn flush(&self) -> Result<()> {
        // Take all pending entries out of the buffer (brief synchronous lock).
        let entries = {
            let mut buf = self
                .buffer
                .lock()
                .map_err(|_| StoreError::InvalidArgument("缓冲区锁定失败".to_string()))?;

            if buf.is_empty() {
                return Ok(());
            }

            std::mem::take(&mut *buf)
        };

        // Serialize the batch payload.
        let payload = serde_json::json!({
            "action": "log_batch",
            "payload": {
                "worker_id": &self.worker_id,
                "entries": entries.iter().map(|e| serde_json::json!({
                    "level":       e.level.to_string(),
                    "category":    e.category.to_string(),
                    "message":     &e.message,
                    "detail_json": &e.detail_json,
                    "timestamp":   e.timestamp.to_rfc3339(),
                })).collect::<Vec<_>>(),
            }
        });

        let msg_text = match serde_json::to_string(&payload) {
            Ok(s) => s,
            Err(e) => {
                // Serialization failure – put entries back.
                self.restore_to_buffer(entries);
                return Err(StoreError::InvalidArgument(format!(
                    "日志序列化失败，已恢复到缓冲区: {}",
                    e
                )));
            }
        };

        // Acquire the WS sink and attempt to send.
        let mut sink_guard = self.ws_sink.lock().await;

        match sink_guard.as_mut() {
            Some(sink) => {
                match sink.send(Message::Text(msg_text)).await {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        // Mark connection as dead and restore entries.
                        *sink_guard = None;
                        drop(sink_guard);
                        self.restore_to_buffer(entries);
                        Err(StoreError::InvalidArgument(format!(
                            "WS 发送失败，日志已恢复到缓冲区: {}",
                            e
                        )))
                    }
                }
            }
            None => {
                // Not yet connected – put entries back.
                drop(sink_guard);
                self.restore_to_buffer(entries);
                Err(StoreError::InvalidArgument(
                    "WS 未连接，日志已恢复到缓冲区".to_string(),
                ))
            }
        }
    }

    // ---- background connection ----

    /// Maintain a persistent WebSocket connection to the Master log server.
    ///
    /// This method loops forever: it connects, drains the read-side
    /// (discarding acknowledgements), and reconnects after any error or
    /// graceful close.  It should be run in its own `tokio::spawn` task.
    pub async fn start_background_connection(&self) {
        let ws_url = format!("ws://{}", self.master_ws_addr);

        loop {
            match tokio_tungstenite::connect_async(&ws_url).await {
                Ok((ws_stream, _)) => {
                    let (mut sink, mut stream) = ws_stream.split();

                    // 连接建立后发送 register 消息，绑定 worker_id
                    let register_msg = format!(
                        r#"{{"action":"register","worker_id":"{}"}}"#,
                        self.worker_id
                    );
                    if sink
                        .send(tokio_tungstenite::tungstenite::Message::Text(register_msg))
                        .await
                        .is_err()
                    {
                        // 注册失败则放弃此连接，等待重连
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        continue;
                    }

                    // Make the new sink available to flush().
                    {
                        let mut guard = self.ws_sink.lock().await;
                        *guard = Some(sink);
                    }

                    // 读取循环：识别 config_update 消息并触发回调
                    while let Some(msg) = stream.next().await {
                        match msg {
                            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                                // 检查是否为 config_update 消息
                                let is_config_update =
                                    serde_json::from_str::<serde_json::Value>(&text)
                                        .ok()
                                        .and_then(|v| {
                                            v.get("type")
                                                .and_then(|t| t.as_str())
                                                .map(|s| s == "config_update")
                                        })
                                        .unwrap_or(false);

                                if is_config_update {
                                    if let Ok(guard) = self.config_update_handler.lock() {
                                        if let Some(handler) = guard.as_ref() {
                                            handler(text);
                                        }
                                    }
                                }
                                // 其他消息（日志 ACK 等）丢弃
                            }
                            Ok(_) => {} // 其他类型消息丢弃
                            Err(e) => {
                                eprintln!("[WorkerLogger] WS 读取错误: {}", e);
                                break;
                            }
                        }
                    }

                    // Connection closed – clear the sink so flush() knows to
                    // buffer entries until the next reconnect.
                    {
                        let mut guard = self.ws_sink.lock().await;
                        *guard = None;
                    }

                    eprintln!("[WorkerLogger] WS 连接断开，5 秒后重连…");
                }
                Err(e) => {
                    eprintln!("[WorkerLogger] WS 连接失败: {} (5 秒后重试)", e);
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }
    }
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- LogLevel / LogCategory round-trip ----

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from("info"), LogLevel::Info);
        assert_eq!(LogLevel::from("INFO"), LogLevel::Info);
        assert_eq!(LogLevel::from("warn"), LogLevel::Warn);
        assert_eq!(LogLevel::from("warning"), LogLevel::Warn);
        assert_eq!(LogLevel::from("error"), LogLevel::Error);
        assert_eq!(LogLevel::from("debug"), LogLevel::Debug);
        assert_eq!(LogLevel::from("unknown"), LogLevel::Info); // default
    }

    #[test]
    fn test_log_level_display() {
        assert_eq!(LogLevel::Info.to_string(), "info");
        assert_eq!(LogLevel::Warn.to_string(), "warn");
        assert_eq!(LogLevel::Error.to_string(), "error");
        assert_eq!(LogLevel::Debug.to_string(), "debug");
    }

    #[test]
    fn test_log_category_from_str() {
        assert_eq!(
            LogCategory::from("system_health"),
            LogCategory::SystemHealth
        );
        assert_eq!(LogCategory::from("network"), LogCategory::Network);
        assert_eq!(LogCategory::from("storage"), LogCategory::Storage);
        assert_eq!(
            LogCategory::from("my_custom"),
            LogCategory::Custom("my_custom".to_string())
        );
    }

    // ---- LogStore CRUD ----

    fn open_in_memory() -> LogStore {
        LogStore::open(":memory:").expect("in-memory LogStore")
    }

    #[test]
    fn test_log_store_write_and_query() {
        let store = open_in_memory();

        store
            .log(
                "worker-1",
                LogLevel::Info,
                LogCategory::SystemHealth,
                "hello",
                None,
            )
            .unwrap();

        let entries = store.query_logs(&LogQuery::default()).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].message, "hello");
        assert_eq!(entries[0].worker_id, "worker-1");
        assert!(!entries[0].acknowledged);
    }

    #[test]
    fn test_log_store_batch_write() {
        let store = open_in_memory();

        let entries: Vec<LogEntry> = (0..5)
            .map(|i| LogEntry {
                id: 0,
                worker_id: "worker-2".to_string(),
                level: LogLevel::Warn,
                category: LogCategory::Network,
                message: format!("msg-{}", i),
                detail_json: None,
                timestamp: Utc::now(),
                acknowledged: false,
            })
            .collect();

        let count = store.write_logs_batch(&entries).unwrap();
        assert_eq!(count, 5);

        let queried = store
            .query_logs(&LogQuery {
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(queried.len(), 5);
    }

    #[test]
    fn test_log_store_stats() {
        let store = open_in_memory();

        store
            .log("w1", LogLevel::Error, LogCategory::Storage, "e1", None)
            .unwrap();
        store
            .log("w1", LogLevel::Info, LogCategory::Network, "i1", None)
            .unwrap();

        let stats = store.get_log_stats().unwrap();
        assert_eq!(stats.total, 2);
        assert_eq!(stats.errors, 1);
        assert_eq!(stats.unread, 2);
        assert_eq!(stats.today, 2);
        assert_eq!(stats.by_worker.len(), 1);
        assert_eq!(stats.by_worker[0].0, "w1");
        assert_eq!(stats.by_worker[0].1, 2);
    }

    #[test]
    fn test_log_store_acknowledge() {
        let store = open_in_memory();

        let id = store
            .log(
                "w1",
                LogLevel::Info,
                LogCategory::Custom("c".to_string()),
                "msg",
                None,
            )
            .unwrap();

        assert!(store.acknowledge_log(id).unwrap());

        let unread = store.get_unread_logs(10).unwrap();
        assert!(unread.is_empty());
    }

    #[test]
    fn test_log_store_query_filter() {
        let store = open_in_memory();

        store
            .log(
                "w1",
                LogLevel::Error,
                LogCategory::Network,
                "network error",
                None,
            )
            .unwrap();
        store
            .log("w1", LogLevel::Info, LogCategory::Storage, "stored", None)
            .unwrap();

        let errors = store
            .query_logs(&LogQuery {
                level: Some(LogLevel::Error),
                limit: 10,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].message, "network error");
    }

    // ---- WorkerLogger buffer preservation ----

    #[test]
    fn test_worker_logger_buffer_push() {
        let logger = WorkerLogger::new("worker-1", "127.0.0.1:9999");
        logger.info(LogCategory::SystemHealth, "hello");
        logger.warn(LogCategory::Network, "world");

        let buf = logger.buffer.lock().unwrap();
        assert_eq!(buf.len(), 2);
        assert_eq!(buf[0].message, "hello");
        assert_eq!(buf[1].message, "world");
    }

    #[test]
    fn test_worker_logger_max_buffer_not_exceeded() {
        let logger = WorkerLogger::new("worker-1", "127.0.0.1:9999").with_max_buffer(3);

        for i in 0..10 {
            logger.info(LogCategory::SystemHealth, &format!("msg-{}", i));
        }

        let buf = logger.buffer.lock().unwrap();
        assert_eq!(buf.len(), 3, "buffer should be capped at max_buffer_size");
    }

    /// Verify that when the WS sink is absent (not connected) `flush` returns
    /// an error **and** all entries are restored to the buffer – no data loss.
    #[tokio::test]
    async fn test_flush_restores_entries_when_not_connected() {
        let logger = WorkerLogger::new("worker-1", "127.0.0.1:9999");

        logger.info(LogCategory::SystemHealth, "a");
        logger.info(LogCategory::SystemHealth, "b");

        // Sink is None (no background connection started).
        let result = logger.flush().await;
        assert!(result.is_err(), "flush must return Err when not connected");

        // Both entries must be back in the buffer.
        let buf = logger.buffer.lock().unwrap();
        assert_eq!(buf.len(), 2, "entries must be restored after failed flush");
    }

    /// Verify that `restore_to_buffer` respects `max_buffer_size` and does
    /// not panic even when called with a large slice.
    #[test]
    fn test_restore_to_buffer_caps_at_max() {
        let logger = WorkerLogger::new("worker-1", "127.0.0.1:9999").with_max_buffer(5);

        // Pre-fill the buffer with 3 entries.
        for i in 0..3 {
            logger.info(LogCategory::SystemHealth, &format!("existing-{}", i));
        }

        // Try to restore 10 entries (only 2 slots remain).
        let to_restore: Vec<LocalLogEntry> = (0..10)
            .map(|i| LocalLogEntry {
                level: LogLevel::Info,
                category: LogCategory::SystemHealth,
                message: format!("restored-{}", i),
                detail_json: None,
                timestamp: Utc::now(),
            })
            .collect();

        logger.restore_to_buffer(to_restore);

        let buf = logger.buffer.lock().unwrap();
        assert_eq!(buf.len(), 5, "buffer should be capped at max_buffer_size");
        // First two entries are the restored ones.
        assert!(buf[0].message.starts_with("restored-"));
        assert!(buf[1].message.starts_with("restored-"));
    }
}
