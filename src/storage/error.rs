#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("not found")]
    NotFound,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("data corruption: {0}")]
    Corruption(String),

    #[error("write conflict")]
    Conflict,

    #[error("read resource limit: {0}")]
    ResourceLimit(String),

    #[error("read timeout: {0}")]
    Timeout(String),

    #[error("read cancelled: {0}")]
    Cancelled(String),

    #[error("storage backend error: {0}")]
    Backend(String),
}
