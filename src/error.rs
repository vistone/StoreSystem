use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("KV database error: {0}")]
    KvError(#[from] jammdb::Error),

    #[error("Meta database error: {0}")]
    MetaError(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Meta not found: {0}")]
    MetaNotFound(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

/// 存储系统 Result 类型别名
pub type Result<T> = std::result::Result<T, StoreError>;
