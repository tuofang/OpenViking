use crate::{YuanrongKvStore, YuanrongStoreError};
use ragfs::cache::{CacheError, CacheResult};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, Semaphore};

pub(crate) struct YuanrongClient {
    stores: RwLock<Vec<Arc<dyn YuanrongKvStore>>>,
    next_store: AtomicUsize,
    concurrency: Arc<Semaphore>,
    concurrency_limit: u32,
    timeout: Duration,
    closed: AtomicBool,
}

impl YuanrongClient {
    pub(crate) fn new(
        stores: Vec<Arc<dyn YuanrongKvStore>>,
        concurrency_limit: usize,
        timeout: Duration,
    ) -> Self {
        assert!(
            !stores.is_empty(),
            "YuanrongClient requires at least one KV store"
        );
        Self {
            stores: RwLock::new(stores),
            next_store: AtomicUsize::new(0),
            concurrency: Arc::new(Semaphore::new(concurrency_limit)),
            concurrency_limit: concurrency_limit as u32,
            timeout,
            closed: AtomicBool::new(false),
        }
    }

    async fn execute<T, F>(&self, operation: &'static str, call: F) -> CacheResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn YuanrongKvStore>) -> Result<T, YuanrongStoreError> + Send + 'static,
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(CacheError::Unavailable(
                "Yuanrong provider is closed".into(),
            ));
        }

        let work = async {
            let permit = Arc::clone(&self.concurrency)
                .acquire_owned()
                .await
                .map_err(|_| CacheError::Unavailable("Yuanrong client is closing".into()))?;
            if self.closed.load(Ordering::Acquire) {
                return Err(CacheError::Unavailable(
                    "Yuanrong provider is closed".into(),
                ));
            }
            let store = self.next_store().await?;
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                call(store)
            })
            .await
            .map_err(|error| {
                CacheError::Internal(format!(
                    "Yuanrong {operation} blocking task failed: {error}"
                ))
            })?
            .map_err(map_store_error)
        };

        tokio::time::timeout(self.timeout, work)
            .await
            .map_err(|_| {
                CacheError::Timeout(format!(
                    "Yuanrong {operation} exceeded {} ms",
                    self.timeout.as_millis()
                ))
            })?
    }

    async fn next_store(&self) -> CacheResult<Arc<dyn YuanrongKvStore>> {
        let stores = self.stores.read().await;
        if stores.is_empty() {
            return Err(CacheError::Unavailable(
                "Yuanrong KV client has been released".into(),
            ));
        }
        let index = self.next_store.fetch_add(1, Ordering::Relaxed) % stores.len();
        Ok(Arc::clone(&stores[index]))
    }

    pub(crate) async fn health_check_all(&self) -> CacheResult<()> {
        let stores = self.stores.read().await.clone();
        if stores.is_empty() {
            return Err(CacheError::Unavailable(
                "Yuanrong KV client has been released".into(),
            ));
        }
        for store in stores {
            self.execute_on_store("health_check", store, |store| store.health_check())
                .await?;
        }
        Ok(())
    }

    async fn execute_on_store<T, F>(
        &self,
        operation: &'static str,
        store: Arc<dyn YuanrongKvStore>,
        call: F,
    ) -> CacheResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn YuanrongKvStore>) -> Result<T, YuanrongStoreError> + Send + 'static,
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(CacheError::Unavailable(
                "Yuanrong provider is closed".into(),
            ));
        }

        let work = async {
            let permit = Arc::clone(&self.concurrency)
                .acquire_owned()
                .await
                .map_err(|_| CacheError::Unavailable("Yuanrong client is closing".into()))?;
            if self.closed.load(Ordering::Acquire) {
                return Err(CacheError::Unavailable(
                    "Yuanrong provider is closed".into(),
                ));
            }
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                call(store)
            })
            .await
            .map_err(|error| {
                CacheError::Internal(format!(
                    "Yuanrong {operation} blocking task failed: {error}"
                ))
            })?
            .map_err(map_store_error)
        };

        tokio::time::timeout(self.timeout, work)
            .await
            .map_err(|_| {
                CacheError::Timeout(format!(
                    "Yuanrong {operation} exceeded {} ms",
                    self.timeout.as_millis()
                ))
            })?
    }

    pub(crate) async fn health_check(&self) -> CacheResult<()> {
        self.execute("health_check", |store| store.health_check())
            .await
    }

    pub(crate) async fn get(&self, key: &str) -> CacheResult<Option<Vec<u8>>> {
        let key = key.to_owned();
        self.execute("get", move |store| store.get(&key)).await
    }

    pub(crate) async fn set(&self, key: &str, value: &[u8]) -> CacheResult<()> {
        let key = key.to_owned();
        let value = value.to_vec();
        self.execute("set", move |store| store.set(&key, &value))
            .await
    }

    pub(crate) async fn delete(&self, key: &str) -> CacheResult<()> {
        let key = key.to_owned();
        self.execute("delete", move |store| store.delete(&key))
            .await
    }

    pub(crate) async fn exists(&self, key: &str) -> CacheResult<bool> {
        let key = key.to_owned();
        self.execute("exists", move |store| store.exists(&key))
            .await
    }

    pub(crate) async fn batch_get(&self, keys: &[String]) -> CacheResult<Vec<Option<Vec<u8>>>> {
        let keys = keys.to_vec();
        self.execute("batch_get", move |store| store.batch_get(&keys))
            .await
    }

    pub(crate) async fn batch_set(&self, entries: Vec<(String, Vec<u8>)>) -> CacheResult<()> {
        self.execute("batch_set", move |store| store.batch_set(&entries))
            .await
    }

    pub(crate) async fn batch_delete(&self, keys: &[String]) -> CacheResult<()> {
        let keys = keys.to_vec();
        self.execute("batch_delete", move |store| store.batch_delete(&keys))
            .await
    }

    pub(crate) async fn close(&self) -> CacheResult<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let permits = Arc::clone(&self.concurrency)
            .acquire_many_owned(self.concurrency_limit)
            .await
            .map_err(|_| CacheError::Unavailable("Yuanrong client is closing".into()))?;
        let stores = {
            let mut stores = self.stores.write().await;
            std::mem::take(&mut *stores)
        };
        let result = if stores.is_empty() {
            Ok(())
        } else {
            tokio::time::timeout(
                self.timeout,
                tokio::task::spawn_blocking(move || {
                    for store in stores {
                        store.shutdown()?;
                    }
                    Ok(())
                }),
            )
            .await
            .map_err(|_| {
                CacheError::Timeout(format!(
                    "Yuanrong shutdown exceeded {} ms",
                    self.timeout.as_millis()
                ))
            })?
            .map_err(|error| {
                CacheError::Internal(format!("Yuanrong shutdown task failed: {error}"))
            })?
            .map_err(map_store_error)
        };
        drop(permits);
        result
    }
}

fn map_store_error(error: YuanrongStoreError) -> CacheError {
    match error {
        YuanrongStoreError::Unavailable(message) => CacheError::Unavailable(message),
        YuanrongStoreError::Timeout(message) => CacheError::Timeout(message),
        YuanrongStoreError::InvalidArgument(message) => CacheError::InvalidArgument(message),
        YuanrongStoreError::Internal(message) => CacheError::Internal(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct CountingStore {
        gets: AtomicUsize,
    }

    impl YuanrongKvStore for CountingStore {
        fn health_check(&self) -> Result<(), YuanrongStoreError> {
            Ok(())
        }

        fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, YuanrongStoreError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            Ok(Some(b"value".to_vec()))
        }

        fn set(&self, _key: &str, _value: &[u8]) -> Result<(), YuanrongStoreError> {
            Ok(())
        }

        fn delete(&self, _key: &str) -> Result<(), YuanrongStoreError> {
            Ok(())
        }

        fn exists(&self, _key: &str) -> Result<bool, YuanrongStoreError> {
            Ok(true)
        }

        fn batch_get(&self, keys: &[String]) -> Result<Vec<Option<Vec<u8>>>, YuanrongStoreError> {
            Ok(keys.iter().map(|_| Some(b"value".to_vec())).collect())
        }

        fn batch_set(&self, _entries: &[(String, Vec<u8>)]) -> Result<(), YuanrongStoreError> {
            Ok(())
        }

        fn batch_delete(&self, _keys: &[String]) -> Result<(), YuanrongStoreError> {
            Ok(())
        }

        fn shutdown(&self) -> Result<(), YuanrongStoreError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn get_calls_are_distributed_across_store_pool() {
        let counters = (0..4)
            .map(|_| Arc::new(CountingStore::default()))
            .collect::<Vec<_>>();
        let stores = counters
            .iter()
            .cloned()
            .map(|store| store as Arc<dyn YuanrongKvStore>)
            .collect::<Vec<_>>();
        let client = YuanrongClient::new(stores.clone(), 4, Duration::from_millis(500));

        for _ in 0..8 {
            assert_eq!(client.get("key").await.unwrap(), Some(b"value".to_vec()));
        }

        let counts = counters
            .iter()
            .map(|store| store.gets.load(Ordering::SeqCst))
            .collect::<Vec<_>>();
        assert_eq!(counts, vec![2, 2, 2, 2]);
    }
}
