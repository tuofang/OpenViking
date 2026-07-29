use crate::MemStoreStoreError;
use bytes::Bytes;

/// Result for one item within a native MemStore batch operation.
pub type MemStoreItemResult<T> = Result<T, MemStoreStoreError>;

/// Synchronous logical KV operations used by the async MemStore provider.
pub trait MemStoreKvStore: Send + Sync + 'static {
    fn health_check(&self) -> Result<(), MemStoreStoreError>;
    fn get(&self, key: &str) -> Result<Option<Bytes>, MemStoreStoreError>;
    fn set(&self, key: &str, value: &[u8]) -> Result<(), MemStoreStoreError>;
    fn delete(&self, key: &str) -> Result<(), MemStoreStoreError>;
    fn exists(&self, key: &str) -> Result<bool, MemStoreStoreError>;
    fn batch_get(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<Option<Bytes>>>, MemStoreStoreError>;
    fn batch_set(
        &self,
        entries: &[(String, Vec<u8>)],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError>;
    fn batch_delete(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError>;
    fn shutdown(&self) -> Result<(), MemStoreStoreError>;
}
