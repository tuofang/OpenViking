/// Errors returned by the synchronous MemStore KV boundary.
#[derive(Debug, thiserror::Error)]
pub enum MemStoreStoreError {
    #[error("MemStore unavailable: {0}")]
    Unavailable(String),
    #[error("MemStore operation timed out: {0}")]
    Timeout(String),
    #[error("invalid MemStore argument: {0}")]
    InvalidArgument(String),
    #[error("invalid MemStore data: {0}")]
    InvalidData(String),
    #[error("MemStore operation failed: {0}")]
    Internal(String),
}
