use crate::client::{map_store_error, MemStoreClient};
use crate::{MemStoreConfig, MemStoreKvStore};
use async_trait::async_trait;
use bytes::Bytes;
use ragfs::cache::{CacheError, CacheProvider, CacheResult, ProviderCapabilities};
use std::collections::HashMap;
use std::fmt;
#[cfg(test)]
use std::sync::OnceLock;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
#[cfg(test)]
use tokio::sync::Barrier;
use tokio::sync::RwLock;

#[derive(Default)]
struct KeyTracker {
    entries: HashMap<String, TrackedKey>,
    next_generation: u64,
}

#[derive(Default)]
struct TrackedKey {
    confirmed: bool,
    pending_writes: usize,
    generation: u64,
}

struct FlushCandidate {
    key: String,
    generation: u64,
}

#[cfg(test)]
#[derive(Clone)]
struct FlushTestHook {
    snapshot_reached: Arc<Barrier>,
    resume: Arc<Barrier>,
}

#[cfg(test)]
fn flush_test_hook() -> &'static Mutex<Option<FlushTestHook>> {
    static HOOK: OnceLock<Mutex<Option<FlushTestHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

impl KeyTracker {
    fn begin_write(&mut self, key: &str) {
        self.next_generation += 1;
        let entry = self.entries.entry(key.to_owned()).or_default();
        entry.pending_writes += 1;
        entry.generation = self.next_generation;
    }

    fn complete_write(&mut self, key: &str, succeeded: bool) {
        let remove = match self.entries.get_mut(key) {
            Some(entry) => {
                entry.pending_writes = entry.pending_writes.saturating_sub(1);
                if succeeded {
                    entry.confirmed = true;
                }
                !entry.confirmed && entry.pending_writes == 0
            }
            None => false,
        };
        if remove {
            self.entries.remove(key);
        }
    }

    fn complete_delete(&mut self, key: &str) {
        let remove = match self.entries.get_mut(key) {
            Some(entry) => {
                entry.confirmed = false;
                entry.pending_writes == 0
            }
            None => false,
        };
        if remove {
            self.entries.remove(key);
        }
    }

    fn flush_candidates(&self) -> Vec<FlushCandidate> {
        self.entries
            .iter()
            .map(|(key, entry)| FlushCandidate {
                key: key.clone(),
                generation: entry.generation,
            })
            .collect()
    }

    fn complete_flush(&mut self, candidate: &FlushCandidate) {
        if self
            .entries
            .get(&candidate.key)
            .is_some_and(|entry| entry.generation <= candidate.generation)
        {
            self.entries.remove(&candidate.key);
        }
    }
}

/// MemStore implementation of the common RAGFS cache provider contract.
pub struct MemStoreProvider {
    client: Arc<MemStoreClient>,
    known_keys: Mutex<KeyTracker>,
    mutation_gate: RwLock<()>,
}

impl fmt::Debug for MemStoreProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemStoreProvider")
            .finish_non_exhaustive()
    }
}

impl MemStoreProvider {
    /// Construct a provider over a MemStore-compatible synchronous KV store.
    pub async fn from_store(
        config: MemStoreConfig,
        store: Arc<dyn MemStoreKvStore>,
    ) -> CacheResult<Self> {
        config.validate()?;
        let client = Arc::new(MemStoreClient::new(
            store,
            config.sdk_concurrency,
            Duration::from_millis(config.operation_timeout_ms),
            config.max_value_size_bytes,
        ));
        client.health_check().await?;
        Ok(Self {
            client,
            known_keys: Mutex::new(KeyTracker::default()),
            mutation_gate: RwLock::new(()),
        })
    }

    /// Return a startup error when native MemStore support is not compiled.
    #[cfg(not(feature = "memstore-native"))]
    pub async fn connect(config: MemStoreConfig) -> CacheResult<Self> {
        config.validate()?;
        Err(CacheError::Unavailable(
            "MemStore support requires the memstore-native feature".into(),
        ))
    }

    /// Check whether the connected MemStore data path is healthy.
    pub async fn health_check(&self) -> CacheResult<()> {
        self.client.health_check().await
    }
}

fn lock_known_keys(known_keys: &Mutex<KeyTracker>) -> CacheResult<MutexGuard<'_, KeyTracker>> {
    known_keys
        .lock()
        .map_err(|_| CacheError::Internal("MemStore key tracker is poisoned".into()))
}

#[async_trait]
impl CacheProvider for MemStoreProvider {
    fn name(&self) -> &'static str {
        "memstore"
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            batch_get: true,
            batch_put: true,
            native_ttl: false,
        }
    }

    async fn get(&self, key: &str) -> CacheResult<Option<Bytes>> {
        Ok(self.client.get(key).await?.map(Bytes::from))
    }

    async fn put(&self, key: &str, value: Bytes) -> CacheResult<()> {
        let _mutation = self.mutation_gate.read().await;
        lock_known_keys(&self.known_keys)?.begin_write(key);
        match self.client.set(key, &value).await {
            Ok(()) => {
                lock_known_keys(&self.known_keys)?.complete_write(key, true);
                Ok(())
            }
            Err(error) => {
                if !matches!(error, CacheError::Timeout(_)) {
                    lock_known_keys(&self.known_keys)?.complete_write(key, false);
                }
                Err(error)
            }
        }
    }

    async fn delete(&self, key: &str) -> CacheResult<()> {
        let _mutation = self.mutation_gate.read().await;
        self.client.delete(key).await?;
        lock_known_keys(&self.known_keys)?.complete_delete(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> CacheResult<bool> {
        self.client.exists(key).await
    }

    async fn batch_get(&self, keys: &[String]) -> CacheResult<Vec<Option<Bytes>>> {
        self.client
            .batch_get(keys)
            .await?
            .into_iter()
            .map(|result| {
                result
                    .map(|value| value.map(Bytes::from))
                    .map_err(map_store_error)
            })
            .collect()
    }

    async fn batch_put(&self, entries: Vec<(String, Bytes)>) -> CacheResult<()> {
        let _mutation = self.mutation_gate.read().await;
        {
            let mut known_keys = lock_known_keys(&self.known_keys)?;
            for (key, _) in &entries {
                known_keys.begin_write(key);
            }
        }
        let store_entries = entries
            .iter()
            .map(|(key, value)| (key.clone(), value.to_vec()))
            .collect();
        let outcomes = match self.client.batch_set(store_entries).await {
            Ok(outcomes) => outcomes,
            Err(error) => {
                if !matches!(error, CacheError::Timeout(_)) {
                    let mut known_keys = lock_known_keys(&self.known_keys)?;
                    for (key, _) in &entries {
                        known_keys.complete_write(key, false);
                    }
                }
                return Err(error);
            }
        };
        let mut first_error = None;
        let mut known_keys = lock_known_keys(&self.known_keys)?;
        for ((key, _), outcome) in entries.into_iter().zip(outcomes) {
            match outcome {
                Ok(()) => {
                    known_keys.complete_write(&key, true);
                }
                Err(error) => {
                    known_keys.complete_write(&key, false);
                    if first_error.is_none() {
                        first_error = Some(map_store_error(error));
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn invalidate(&self, keys: &[String]) -> CacheResult<()> {
        let _mutation = self.mutation_gate.read().await;
        let outcomes = self.client.batch_delete(keys).await?;
        let mut first_error = None;
        let mut known_keys = lock_known_keys(&self.known_keys)?;
        for (key, outcome) in keys.iter().zip(outcomes) {
            match outcome {
                Ok(()) => {
                    known_keys.complete_delete(key);
                }
                Err(error) if first_error.is_none() => first_error = Some(map_store_error(error)),
                Err(_) => {}
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn flush(&self) -> CacheResult<()> {
        let _flush = self.mutation_gate.write().await;
        let candidates = lock_known_keys(&self.known_keys)?.flush_candidates();
        #[cfg(test)]
        let hook = flush_test_hook()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone();
        #[cfg(test)]
        if let Some(hook) = hook {
            hook.snapshot_reached.wait().await;
            hook.resume.wait().await;
        }
        let keys = candidates
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect::<Vec<_>>();
        let outcomes = self.client.flush_delete(&keys).await?;
        let mut first_error = None;
        let mut known_keys = lock_known_keys(&self.known_keys)?;
        for (candidate, outcome) in candidates.iter().zip(outcomes) {
            match outcome {
                Ok(()) => known_keys.complete_flush(candidate),
                Err(error) if first_error.is_none() => first_error = Some(map_store_error(error)),
                Err(_) => {}
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn close(&self) -> CacheResult<()> {
        self.client.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemStoreItemResult, MemStoreStoreError};

    #[derive(Default)]
    struct TestStore {
        values: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MemStoreKvStore for TestStore {
        fn health_check(&self) -> Result<(), MemStoreStoreError> {
            Ok(())
        }

        fn get(&self, key: &str) -> Result<Option<Vec<u8>>, MemStoreStoreError> {
            Ok(self.values.lock().unwrap().get(key).cloned())
        }

        fn set(&self, key: &str, value: &[u8]) -> Result<(), MemStoreStoreError> {
            self.values
                .lock()
                .unwrap()
                .insert(key.to_owned(), value.to_vec());
            Ok(())
        }

        fn delete(&self, key: &str) -> Result<(), MemStoreStoreError> {
            self.values.lock().unwrap().remove(key);
            Ok(())
        }

        fn exists(&self, key: &str) -> Result<bool, MemStoreStoreError> {
            Ok(self.values.lock().unwrap().contains_key(key))
        }

        fn batch_get(
            &self,
            keys: &[String],
        ) -> Result<Vec<MemStoreItemResult<Option<Vec<u8>>>>, MemStoreStoreError> {
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
            self.values.lock().unwrap().extend(entries.iter().cloned());
            Ok(entries.iter().map(|_| Ok(())).collect())
        }

        fn batch_delete(
            &self,
            keys: &[String],
        ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError> {
            let mut values = self.values.lock().unwrap();
            for key in keys {
                values.remove(key);
            }
            Ok(keys.iter().map(|_| Ok(())).collect())
        }

        fn shutdown(&self) -> Result<(), MemStoreStoreError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn flush_does_not_delete_a_put_started_after_its_snapshot() {
        let store = Arc::new(TestStore::default());
        let provider = Arc::new(
            MemStoreProvider::from_store(
                MemStoreConfig {
                    operation_timeout_ms: 500,
                    ..MemStoreConfig::default()
                },
                store,
            )
            .await
            .unwrap(),
        );
        provider
            .put("shared", Bytes::from_static(b"old"))
            .await
            .unwrap();
        let hook = FlushTestHook {
            snapshot_reached: Arc::new(Barrier::new(2)),
            resume: Arc::new(Barrier::new(2)),
        };
        *flush_test_hook().lock().unwrap() = Some(hook.clone());

        let flush_task = {
            let provider = provider.clone();
            tokio::spawn(async move { provider.flush().await })
        };
        hook.snapshot_reached.wait().await;
        let put_task = {
            let provider = provider.clone();
            tokio::spawn(async move { provider.put("shared", Bytes::from_static(b"new")).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        let put_completed_while_flush_was_paused = put_task.is_finished();
        hook.resume.wait().await;

        flush_task.await.unwrap().unwrap();
        put_task.await.unwrap().unwrap();
        *flush_test_hook().lock().unwrap() = None;
        assert_eq!(
            provider.get("shared").await.unwrap(),
            Some(Bytes::from_static(b"new"))
        );
        assert!(!put_completed_while_flush_was_paused);
    }
}
