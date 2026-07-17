use bytes::Bytes;
use ragfs::cache::{CacheError, CacheProvider};
use ragfs_cache_memstore::{
    MemStoreConfig, MemStoreItemResult, MemStoreKvStore, MemStoreProvider, MemStoreStoreError,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const NO_FAILURE: usize = usize::MAX;

#[derive(Default)]
struct FakeKvStore {
    values: Mutex<HashMap<String, Vec<u8>>>,
    healthy: AtomicBool,
    available: AtomicBool,
    delay_ms: AtomicUsize,
    set_delay_ms: AtomicUsize,
    batch_set_delay_ms: AtomicUsize,
    batch_delete_delay_ms: AtomicUsize,
    shutdown_delay_ms: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    batch_get_calls: AtomicUsize,
    batch_set_calls: AtomicUsize,
    batch_delete_calls: AtomicUsize,
    shutdown_calls: AtomicUsize,
    batch_set_failure: AtomicUsize,
    batch_delete_failure: AtomicUsize,
    observed_batch_get: Mutex<Vec<Vec<String>>>,
    observed_batch_delete: Mutex<Vec<Vec<String>>>,
}

impl FakeKvStore {
    fn available() -> Self {
        Self {
            healthy: AtomicBool::new(true),
            available: AtomicBool::new(true),
            batch_set_failure: AtomicUsize::new(NO_FAILURE),
            batch_delete_failure: AtomicUsize::new(NO_FAILURE),
            ..Self::default()
        }
    }

    fn enter(&self) -> Result<ActiveGuard<'_>, MemStoreStoreError> {
        if !self.available.load(Ordering::SeqCst) {
            return Err(MemStoreStoreError::Unavailable(
                "MemStore unavailable".into(),
            ));
        }
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        let delay = self.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay as u64));
        }
        Ok(ActiveGuard { store: self })
    }

    fn delay(delay_ms: &AtomicUsize) {
        let delay = delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            std::thread::sleep(Duration::from_millis(delay as u64));
        }
    }
}

struct ActiveGuard<'a> {
    store: &'a FakeKvStore,
}

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.store.active.fetch_sub(1, Ordering::SeqCst);
    }
}

impl MemStoreKvStore for FakeKvStore {
    fn health_check(&self) -> Result<(), MemStoreStoreError> {
        let _guard = self.enter()?;
        if self.healthy.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(MemStoreStoreError::Unavailable(
                "MemStore health check failed".into(),
            ))
        }
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, MemStoreStoreError> {
        let _guard = self.enter()?;
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<(), MemStoreStoreError> {
        let _guard = self.enter()?;
        Self::delay(&self.set_delay_ms);
        self.values
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), MemStoreStoreError> {
        let _guard = self.enter()?;
        self.values.lock().unwrap().remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool, MemStoreStoreError> {
        let _guard = self.enter()?;
        Ok(self.values.lock().unwrap().contains_key(key))
    }

    fn batch_get(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<Option<Vec<u8>>>>, MemStoreStoreError> {
        let _guard = self.enter()?;
        self.batch_get_calls.fetch_add(1, Ordering::SeqCst);
        self.observed_batch_get.lock().unwrap().push(keys.to_vec());
        let values = self.values.lock().unwrap();
        Ok(keys
            .iter()
            .map(|key| Ok(values.get(key).cloned()))
            .collect())
    }

    fn batch_set(
        &self,
        entries: &[(String, Vec<u8>)],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError> {
        let _guard = self.enter()?;
        Self::delay(&self.batch_set_delay_ms);
        self.batch_set_calls.fetch_add(1, Ordering::SeqCst);
        let failure = self.batch_set_failure.load(Ordering::SeqCst);
        let mut values = self.values.lock().unwrap();
        Ok(entries
            .iter()
            .enumerate()
            .map(|(index, (key, value))| {
                if index == failure {
                    Err(MemStoreStoreError::Unavailable("item failed".into()))
                } else {
                    values.insert(key.clone(), value.clone());
                    Ok(())
                }
            })
            .collect())
    }

    fn batch_delete(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError> {
        let _guard = self.enter()?;
        Self::delay(&self.batch_delete_delay_ms);
        self.batch_delete_calls.fetch_add(1, Ordering::SeqCst);
        self.observed_batch_delete
            .lock()
            .unwrap()
            .push(keys.to_vec());
        let failure = self.batch_delete_failure.load(Ordering::SeqCst);
        let mut values = self.values.lock().unwrap();
        Ok(keys
            .iter()
            .enumerate()
            .map(|(index, key)| {
                if index == failure {
                    Err(MemStoreStoreError::Unavailable("item failed".into()))
                } else {
                    values.remove(key);
                    Ok(())
                }
            })
            .collect())
    }

    fn shutdown(&self) -> Result<(), MemStoreStoreError> {
        Self::delay(&self.shutdown_delay_ms);
        self.shutdown_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn config() -> MemStoreConfig {
    MemStoreConfig {
        sdk_concurrency: 2,
        operation_timeout_ms: 100,
        max_value_size_bytes: 1_024,
        ..MemStoreConfig::default()
    }
}

async fn provider(store: Arc<FakeKvStore>) -> MemStoreProvider {
    MemStoreProvider::from_store(config(), store).await.unwrap()
}

#[tokio::test]
async fn initialization_validates_config_and_health() {
    let mut invalid = config();
    invalid.sdk_concurrency = 0;
    let error = MemStoreProvider::from_store(invalid, Arc::new(FakeKvStore::available()))
        .await
        .unwrap_err();
    assert!(matches!(error, CacheError::InvalidArgument(_)));

    let unhealthy = Arc::new(FakeKvStore::available());
    unhealthy.healthy.store(false, Ordering::SeqCst);
    let error = MemStoreProvider::from_store(config(), unhealthy)
        .await
        .unwrap_err();
    assert!(matches!(error, CacheError::Unavailable(_)));
}

#[tokio::test]
async fn hit_miss_upsert_empty_payload_delete_twice_and_exists_work() {
    let store = Arc::new(FakeKvStore::available());
    store
        .values
        .lock()
        .unwrap()
        .insert("hit".into(), b"value".to_vec());
    let provider = provider(store).await;

    assert_eq!(provider.name(), "memstore");
    assert_eq!(
        provider.get("hit").await.unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(provider.get("missing").await.unwrap(), None);
    provider.put("written", Bytes::new()).await.unwrap();
    assert_eq!(provider.get("written").await.unwrap(), Some(Bytes::new()));
    provider
        .put("written", Bytes::from_static(b"replacement"))
        .await
        .unwrap();
    assert!(provider.exists("written").await.unwrap());
    assert_eq!(
        provider.get("written").await.unwrap(),
        Some(Bytes::from_static(b"replacement"))
    );
    provider.delete("written").await.unwrap();
    provider.delete("written").await.unwrap();
    assert!(!provider.exists("written").await.unwrap());
}

#[tokio::test]
async fn invalid_keys_and_oversized_payloads_are_rejected_before_store_calls() {
    let store = Arc::new(FakeKvStore::available());
    let provider = provider(store.clone()).await;

    assert!(matches!(
        provider.get("").await.unwrap_err(),
        CacheError::InvalidArgument(_)
    ));
    assert!(matches!(
        provider
            .put("large", Bytes::from(vec![0; 1_025]))
            .await
            .unwrap_err(),
        CacheError::InvalidArgument(_)
    ));
    assert!(store.values.lock().unwrap().is_empty());
}

#[tokio::test]
async fn batch_operations_use_native_calls_preserve_order_and_delete_missing_keys() {
    let store = Arc::new(FakeKvStore::available());
    let provider = provider(store.clone()).await;

    provider
        .batch_put(vec![
            ("one".into(), Bytes::from_static(b"1")),
            ("two".into(), Bytes::from_static(b"2")),
        ])
        .await
        .unwrap();
    let keys = vec!["two".into(), "missing".into(), "one".into()];
    assert_eq!(
        provider.batch_get(&keys).await.unwrap(),
        vec![
            Some(Bytes::from_static(b"2")),
            None,
            Some(Bytes::from_static(b"1"))
        ]
    );
    provider
        .invalidate(&["one".into(), "already-missing".into(), "two".into()])
        .await
        .unwrap();

    assert_eq!(store.batch_set_calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.batch_get_calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.batch_delete_calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.observed_batch_get.lock().unwrap().as_slice(), &[keys]);
    assert!(provider.capabilities().batch_get);
    assert!(provider.capabilities().batch_put);
    assert!(!provider.capabilities().native_ttl);
}

#[tokio::test]
async fn partial_batch_failure_returns_error_and_tracks_only_successful_items() {
    let store = Arc::new(FakeKvStore::available());
    store
        .values
        .lock()
        .unwrap()
        .insert("failed".into(), b"original".to_vec());
    store.batch_set_failure.store(1, Ordering::SeqCst);
    let provider = provider(store.clone()).await;

    let error = provider
        .batch_put(vec![
            ("first".into(), Bytes::from_static(b"1")),
            ("failed".into(), Bytes::from_static(b"new")),
            ("third".into(), Bytes::from_static(b"3")),
        ])
        .await
        .unwrap_err();
    assert!(matches!(error, CacheError::Unavailable(_)));

    store.batch_set_failure.store(NO_FAILURE, Ordering::SeqCst);
    provider.flush().await.unwrap();
    let values = store.values.lock().unwrap();
    assert!(!values.contains_key("first"));
    assert!(!values.contains_key("third"));
    assert_eq!(values.get("failed").unwrap(), b"original");
    let mut deleted = store.observed_batch_delete.lock().unwrap()[0].clone();
    deleted.sort();
    assert_eq!(deleted, vec![String::from("first"), String::from("third")]);
}

#[tokio::test]
async fn partial_batch_delete_failure_keeps_only_failed_items_tracked() {
    let store = Arc::new(FakeKvStore::available());
    let provider = provider(store.clone()).await;
    provider
        .batch_put(vec![
            ("first".into(), Bytes::from_static(b"1")),
            ("failed".into(), Bytes::from_static(b"2")),
            ("third".into(), Bytes::from_static(b"3")),
        ])
        .await
        .unwrap();
    store.batch_delete_failure.store(1, Ordering::SeqCst);

    let error = provider
        .invalidate(&["first".into(), "failed".into(), "third".into()])
        .await
        .unwrap_err();
    assert!(matches!(error, CacheError::Unavailable(_)));

    store
        .batch_delete_failure
        .store(NO_FAILURE, Ordering::SeqCst);
    provider.flush().await.unwrap();
    assert!(store.values.lock().unwrap().is_empty());
    assert_eq!(
        store.observed_batch_delete.lock().unwrap().as_slice(),
        &[
            vec![
                String::from("first"),
                String::from("failed"),
                String::from("third"),
            ],
            vec![String::from("failed")],
        ]
    );
}

#[tokio::test]
async fn synchronous_calls_are_bounded_and_timeout_is_not_a_miss() {
    let store = Arc::new(FakeKvStore::available());
    store
        .values
        .lock()
        .unwrap()
        .insert("slow".into(), b"value".to_vec());
    store.delay_ms.store(30, Ordering::SeqCst);
    let mut bounded = config();
    bounded.operation_timeout_ms = 500;
    let provider = Arc::new(
        MemStoreProvider::from_store(bounded, store.clone())
            .await
            .unwrap(),
    );

    let tasks = (0..8)
        .map(|_| {
            let provider = provider.clone();
            tokio::spawn(async move { provider.get("slow").await.unwrap() })
        })
        .collect::<Vec<_>>();
    for task in tasks {
        assert_eq!(task.await.unwrap(), Some(Bytes::from_static(b"value")));
    }
    assert!(store.max_active.load(Ordering::SeqCst) <= 2);

    let timeout_store = Arc::new(FakeKvStore::available());
    let mut timeout_config = config();
    timeout_config.operation_timeout_ms = 10;
    let provider = MemStoreProvider::from_store(timeout_config, timeout_store.clone())
        .await
        .unwrap();
    timeout_store.delay_ms.store(80, Ordering::SeqCst);
    let error = provider.get("slow").await.unwrap_err();
    assert!(matches!(error, CacheError::Timeout(_)));
}

#[tokio::test]
async fn timed_out_single_write_is_retained_for_flush_until_late_completion() {
    let store = Arc::new(FakeKvStore::available());
    let mut timeout_config = config();
    timeout_config.operation_timeout_ms = 10;
    let provider = MemStoreProvider::from_store(timeout_config, store.clone())
        .await
        .unwrap();
    store.set_delay_ms.store(80, Ordering::SeqCst);

    assert!(matches!(
        provider
            .put("late-single", Bytes::from_static(b"value"))
            .await
            .unwrap_err(),
        CacheError::Timeout(_)
    ));
    provider.flush().await.unwrap();

    assert!(!store.values.lock().unwrap().contains_key("late-single"));
    assert_eq!(store.batch_delete_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn timed_out_batch_write_is_retained_for_flush_until_late_completion() {
    let store = Arc::new(FakeKvStore::available());
    let mut timeout_config = config();
    timeout_config.operation_timeout_ms = 10;
    let provider = MemStoreProvider::from_store(timeout_config, store.clone())
        .await
        .unwrap();
    store.batch_set_delay_ms.store(80, Ordering::SeqCst);

    assert!(matches!(
        provider
            .batch_put(vec![
                ("late-first".into(), Bytes::from_static(b"1")),
                ("late-second".into(), Bytes::from_static(b"2")),
            ])
            .await
            .unwrap_err(),
        CacheError::Timeout(_)
    ));
    provider.flush().await.unwrap();

    assert!(store.values.lock().unwrap().is_empty());
    assert_eq!(store.batch_delete_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn close_is_idempotent_waits_for_admitted_work_and_rejects_new_operations() {
    let store = Arc::new(FakeKvStore::available());
    store
        .values
        .lock()
        .unwrap()
        .insert("slow".into(), b"value".to_vec());
    let mut close_config = config();
    close_config.operation_timeout_ms = 500;
    let provider = Arc::new(
        MemStoreProvider::from_store(close_config, store.clone())
            .await
            .unwrap(),
    );
    store.delay_ms.store(80, Ordering::SeqCst);

    let get_task = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.get("slow").await })
    };
    while store.active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let close_task = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.close().await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(store.shutdown_calls.load(Ordering::SeqCst), 0);

    assert_eq!(
        get_task.await.unwrap().unwrap(),
        Some(Bytes::from_static(b"value"))
    );
    close_task.await.unwrap().unwrap();
    provider.close().await.unwrap();
    assert_eq!(store.shutdown_calls.load(Ordering::SeqCst), 1);
    assert!(matches!(
        provider.get("key").await.unwrap_err(),
        CacheError::Unavailable(_)
    ));
}

#[tokio::test]
async fn concurrent_close_callers_wait_for_the_same_shutdown_completion() {
    let store = Arc::new(FakeKvStore::available());
    store
        .values
        .lock()
        .unwrap()
        .insert("slow".into(), b"value".to_vec());
    let mut close_config = config();
    close_config.operation_timeout_ms = 500;
    let provider = Arc::new(
        MemStoreProvider::from_store(close_config, store.clone())
            .await
            .unwrap(),
    );
    store.delay_ms.store(80, Ordering::SeqCst);
    let get_task = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.get("slow").await })
    };
    while store.active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    let first_close = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.close().await })
    };
    let second_close = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.close().await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!first_close.is_finished());
    assert!(!second_close.is_finished());
    assert_eq!(store.shutdown_calls.load(Ordering::SeqCst), 0);

    get_task.await.unwrap().unwrap();
    first_close.await.unwrap().unwrap();
    second_close.await.unwrap().unwrap();
    assert_eq!(store.shutdown_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cancelling_first_close_does_not_abandon_drain_and_shutdown() {
    let store = Arc::new(FakeKvStore::available());
    store
        .values
        .lock()
        .unwrap()
        .insert("slow".into(), b"value".to_vec());
    let mut close_config = config();
    close_config.operation_timeout_ms = 500;
    let provider = Arc::new(
        MemStoreProvider::from_store(close_config, store.clone())
            .await
            .unwrap(),
    );
    store.delay_ms.store(80, Ordering::SeqCst);
    let get_task = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.get("slow").await })
    };
    while store.active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }

    let first_close = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.close().await })
    };
    tokio::time::sleep(Duration::from_millis(10)).await;
    first_close.abort();
    assert!(first_close.await.unwrap_err().is_cancelled());
    get_task.await.unwrap().unwrap();

    tokio::time::timeout(Duration::from_millis(200), async {
        while store.shutdown_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    provider.close().await.unwrap();
    assert_eq!(store.shutdown_calls.load(Ordering::SeqCst), 1);
}

#[cfg(not(feature = "memstore-native"))]
#[tokio::test]
async fn connect_without_native_feature_returns_exact_startup_error() {
    let error = MemStoreProvider::connect(config()).await.unwrap_err();
    assert!(matches!(
        error,
        CacheError::Unavailable(message)
            if message == "MemStore support requires the memstore-native feature"
    ));
}
