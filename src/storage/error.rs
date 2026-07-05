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

    #[error("storage backend error: {0}")]
    Backend(String),
}
