use crate::frame::validate_key;
use crate::{MemStoreItemResult, MemStoreKvStore, MemStoreStoreError};
use ragfs::cache::{CacheError, CacheResult};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, RwLock, Semaphore};

struct ShutdownState {
    started: AtomicBool,
    result: Mutex<Option<CacheResult<()>>>,
    completed: Notify,
}

impl ShutdownState {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            result: Mutex::new(None),
            completed: Notify::new(),
        }
    }

    fn finish(&self, result: CacheResult<()>) {
        *self
            .result
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(result);
        self.completed.notify_waiters();
    }

    async fn wait(&self) -> CacheResult<()> {
        loop {
            let completed = self.completed.notified();
            tokio::pin!(completed);
            completed.as_mut().enable();
            if let Some(result) = self
                .result
                .lock()
                .map_err(|_| CacheError::Internal("MemStore shutdown state is poisoned".into()))?
                .as_ref()
            {
                return clone_cache_result(result);
            }
            completed.await;
        }
    }
}

pub(crate) struct MemStoreClient {
    store: Arc<RwLock<Option<Arc<dyn MemStoreKvStore>>>>,
    concurrency: Arc<Semaphore>,
    concurrency_limit: u32,
    timeout: Duration,
    max_value_size: usize,
    closed: AtomicBool,
    shutdown: Arc<ShutdownState>,
}

impl MemStoreClient {
    pub(crate) fn new(
        store: Arc<dyn MemStoreKvStore>,
        concurrency_limit: usize,
        timeout: Duration,
        max_value_size: usize,
    ) -> Self {
        Self {
            store: Arc::new(RwLock::new(Some(store))),
            concurrency: Arc::new(Semaphore::new(concurrency_limit)),
            concurrency_limit: concurrency_limit as u32,
            timeout,
            max_value_size,
            closed: AtomicBool::new(false),
            shutdown: Arc::new(ShutdownState::new()),
        }
    }

    async fn execute<T, F>(&self, operation: &'static str, call: F) -> CacheResult<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<dyn MemStoreKvStore>) -> Result<T, MemStoreStoreError> + Send + 'static,
    {
        if self.closed.load(Ordering::Acquire) {
            return Err(CacheError::Unavailable(
                "MemStore provider is closed".into(),
            ));
        }

        let work =
            async {
                let permit = Arc::clone(&self.concurrency)
                    .acquire_owned()
                    .await
                    .map_err(|_| CacheError::Unavailable("MemStore client is closing".into()))?;
                if self.closed.load(Ordering::Acquire) {
                    return Err(CacheError::Unavailable(
                        "MemStore provider is closed".into(),
                    ));
                }
                let store =
                    self.store.read().await.clone().ok_or_else(|| {
                        CacheError::Unavailable("MemStore store was released".into())
                    })?;
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    call(store)
                })
                .await
                .map_err(|error| {
                    CacheError::Internal(format!(
                        "MemStore {operation} blocking task failed: {error}"
                    ))
                })?
                .map_err(map_store_error)
            };

        tokio::time::timeout(self.timeout, work)
            .await
            .map_err(|_| {
                CacheError::Timeout(format!(
                    "MemStore {operation} exceeded {} ms",
                    self.timeout.as_millis()
                ))
            })?
    }

    pub(crate) async fn health_check(&self) -> CacheResult<()> {
        self.execute("health_check", |store| store.health_check())
            .await
    }

    pub(crate) async fn get(&self, key: &str) -> CacheResult<Option<Vec<u8>>> {
        validate_key(key).map_err(map_store_error)?;
        let key = key.to_owned();
        self.execute("get", move |store| store.get(&key)).await
    }

    pub(crate) async fn set(&self, key: &str, value: &[u8]) -> CacheResult<()> {
        validate_key(key).map_err(map_store_error)?;
        self.validate_payload(value)?;
        let key = key.to_owned();
        let value = value.to_vec();
        self.execute("set", move |store| store.set(&key, &value))
            .await
    }

    pub(crate) async fn delete(&self, key: &str) -> CacheResult<()> {
        validate_key(key).map_err(map_store_error)?;
        let key = key.to_owned();
        self.execute("delete", move |store| store.delete(&key))
            .await
    }

    pub(crate) async fn exists(&self, key: &str) -> CacheResult<bool> {
        validate_key(key).map_err(map_store_error)?;
        let key = key.to_owned();
        self.execute("exists", move |store| store.exists(&key))
            .await
    }

    pub(crate) async fn batch_get(
        &self,
        keys: &[String],
    ) -> CacheResult<Vec<MemStoreItemResult<Option<Vec<u8>>>>> {
        for key in keys {
            validate_key(key).map_err(map_store_error)?;
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys.to_vec();
        let expected = keys.len();
        let results = self
            .execute("batch_get", move |store| store.batch_get(&keys))
            .await?;
        validate_batch_len("batch_get", expected, results.len())?;
        Ok(results)
    }

    pub(crate) async fn batch_set(
        &self,
        entries: Vec<(String, Vec<u8>)>,
    ) -> CacheResult<Vec<MemStoreItemResult<()>>> {
        for (key, value) in &entries {
            validate_key(key).map_err(map_store_error)?;
            self.validate_payload(value)?;
        }
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let expected = entries.len();
        let results = self
            .execute("batch_set", move |store| store.batch_set(&entries))
            .await?;
        validate_batch_len("batch_set", expected, results.len())?;
        Ok(results)
    }

    pub(crate) async fn batch_delete(
        &self,
        keys: &[String],
    ) -> CacheResult<Vec<MemStoreItemResult<()>>> {
        for key in keys {
            validate_key(key).map_err(map_store_error)?;
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let keys = keys.to_vec();
        let expected = keys.len();
        let results = self
            .execute("batch_delete", move |store| store.batch_delete(&keys))
            .await?;
        validate_batch_len("batch_delete", expected, results.len())?;
        Ok(results)
    }

    pub(crate) async fn flush_delete(
        &self,
        keys: &[String],
    ) -> CacheResult<Vec<MemStoreItemResult<()>>> {
        for key in keys {
            validate_key(key).map_err(map_store_error)?;
        }
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(CacheError::Unavailable(
                "MemStore provider is closed".into(),
            ));
        }

        let permits = Arc::clone(&self.concurrency)
            .acquire_many_owned(self.concurrency_limit)
            .await
            .map_err(|_| CacheError::Unavailable("MemStore client is closing".into()))?;
        if self.closed.load(Ordering::Acquire) {
            return Err(CacheError::Unavailable(
                "MemStore provider is closed".into(),
            ));
        }
        let store = self
            .store
            .read()
            .await
            .clone()
            .ok_or_else(|| CacheError::Unavailable("MemStore store was released".into()))?;
        let keys = keys.to_vec();
        let expected = keys.len();
        let results = tokio::time::timeout(
            self.timeout,
            tokio::task::spawn_blocking(move || {
                let _permits = permits;
                store.batch_delete(&keys)
            }),
        )
        .await
        .map_err(|_| {
            CacheError::Timeout(format!(
                "MemStore flush exceeded {} ms",
                self.timeout.as_millis()
            ))
        })?
        .map_err(|error| CacheError::Internal(format!("MemStore flush task failed: {error}")))?
        .map_err(map_store_error)?;
        validate_batch_len("flush", expected, results.len())?;
        Ok(results)
    }

    pub(crate) async fn close(&self) -> CacheResult<()> {
        self.closed.store(true, Ordering::Release);
        if !self.shutdown.started.swap(true, Ordering::AcqRel) {
            let store = Arc::clone(&self.store);
            let concurrency = Arc::clone(&self.concurrency);
            let concurrency_limit = self.concurrency_limit;
            let timeout = self.timeout;
            let shutdown = Arc::clone(&self.shutdown);
            tokio::spawn(async move {
                let result = shutdown_store(store, concurrency, concurrency_limit, timeout).await;
                shutdown.finish(result);
            });
        }
        self.shutdown.wait().await
    }

    fn validate_payload(&self, value: &[u8]) -> CacheResult<()> {
        if value.len() > self.max_value_size {
            Err(CacheError::InvalidArgument(format!(
                "MemStore payload is {} bytes, exceeding the {} byte limit",
                value.len(),
                self.max_value_size
            )))
        } else {
            Ok(())
        }
    }
}

async fn shutdown_store(
    store: Arc<RwLock<Option<Arc<dyn MemStoreKvStore>>>>,
    concurrency: Arc<Semaphore>,
    concurrency_limit: u32,
    timeout: Duration,
) -> CacheResult<()> {
    let permits = concurrency
        .acquire_many_owned(concurrency_limit)
        .await
        .map_err(|_| CacheError::Unavailable("MemStore client is closing".into()))?;
    let store = store.write().await.take();
    let result = match store {
        Some(store) => tokio::time::timeout(
            timeout,
            tokio::task::spawn_blocking(move || store.shutdown()),
        )
        .await
        .map_err(|_| {
            CacheError::Timeout(format!(
                "MemStore shutdown exceeded {} ms",
                timeout.as_millis()
            ))
        })?
        .map_err(|error| CacheError::Internal(format!("MemStore shutdown task failed: {error}")))?
        .map_err(map_store_error),
        None => Ok(()),
    };
    drop(permits);
    result
}

fn clone_cache_result(result: &CacheResult<()>) -> CacheResult<()> {
    match result {
        Ok(()) => Ok(()),
        Err(CacheError::Unavailable(message)) => Err(CacheError::Unavailable(message.clone())),
        Err(CacheError::Timeout(message)) => Err(CacheError::Timeout(message.clone())),
        Err(CacheError::InvalidData(message)) => Err(CacheError::InvalidData(message.clone())),
        Err(CacheError::InvalidArgument(message)) => {
            Err(CacheError::InvalidArgument(message.clone()))
        }
        Err(CacheError::Internal(message)) => Err(CacheError::Internal(message.clone())),
    }
}

fn validate_batch_len(operation: &str, expected: usize, actual: usize) -> CacheResult<()> {
    if expected == actual {
        Ok(())
    } else {
        Err(CacheError::Internal(format!(
            "MemStore {operation} returned {actual} item results for {expected} inputs"
        )))
    }
}

pub(crate) fn map_store_error(error: MemStoreStoreError) -> CacheError {
    match error {
        MemStoreStoreError::Unavailable(message) => CacheError::Unavailable(message),
        MemStoreStoreError::Timeout(message) => CacheError::Timeout(message),
        MemStoreStoreError::InvalidArgument(message) => CacheError::InvalidArgument(message),
        MemStoreStoreError::InvalidData(message) => CacheError::InvalidData(message),
        MemStoreStoreError::Internal(message) => CacheError::Internal(message),
    }
}
