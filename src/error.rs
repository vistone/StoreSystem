use thiserror::Error;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("KV database error: {0}")]
    KvError(#[from] jammdb::Error),

    #[error("Meta database error: {0}")]
    MetaError(#[from] rusqlite::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Meta not found: {0}")]
    MetaNotFound(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;
