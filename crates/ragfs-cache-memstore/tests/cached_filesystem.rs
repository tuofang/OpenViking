use async_trait::async_trait;
use ragfs::cache::{
    CacheNamespace, CachePolicy, CacheProvider, CacheTraversalMode, CachedFileSystem,
};
use ragfs::core::{GrepResult, TreeEntry};
use ragfs::plugins::MemFileSystem;
use ragfs::{FileInfo, FileSystem, Result as FsResult, WriteFlag};
use ragfs_cache_memstore::{
    MemStoreConfig, MemStoreItemResult, MemStoreKvStore, MemStoreProvider, MemStoreStoreError,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct SharedKvStore {
    values: Mutex<HashMap<String, Vec<u8>>>,
    unavailable: AtomicBool,
    batch_get_calls: AtomicUsize,
}

impl SharedKvStore {
    fn check(&self) -> Result<(), MemStoreStoreError> {
        if self.unavailable.load(Ordering::SeqCst) {
            Err(MemStoreStoreError::Unavailable(
                "MemStore unavailable".into(),
            ))
        } else {
            Ok(())
        }
    }
}

impl MemStoreKvStore for SharedKvStore {
    fn health_check(&self) -> Result<(), MemStoreStoreError> {
        self.check()
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, MemStoreStoreError> {
        self.check()?;
        Ok(self.values.lock().unwrap().get(key).cloned())
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<(), MemStoreStoreError> {
        self.check()?;
        self.values
            .lock()
            .unwrap()
            .insert(key.to_owned(), value.to_vec());
        Ok(())
    }

    fn delete(&self, key: &str) -> Result<(), MemStoreStoreError> {
        self.check()?;
        self.values.lock().unwrap().remove(key);
        Ok(())
    }

    fn exists(&self, key: &str) -> Result<bool, MemStoreStoreError> {
        self.check()?;
        Ok(self.values.lock().unwrap().contains_key(key))
    }

    fn batch_get(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<Option<Vec<u8>>>>, MemStoreStoreError> {
        self.check()?;
        self.batch_get_calls.fetch_add(1, Ordering::SeqCst);
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
        self.check()?;
        self.values.lock().unwrap().extend(entries.iter().cloned());
        Ok(entries.iter().map(|_| Ok(())).collect())
    }

    fn batch_delete(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError> {
        self.check()?;
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

#[derive(Clone)]
struct CountingFileSystem {
    inner: Arc<MemFileSystem>,
    reads: Arc<AtomicU64>,
    read_dirs: Arc<AtomicU64>,
}

impl CountingFileSystem {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemFileSystem::new()),
            reads: Arc::new(AtomicU64::new(0)),
            read_dirs: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[async_trait]
impl FileSystem for CountingFileSystem {
    async fn create(&self, path: &str) -> FsResult<()> {
        self.inner.create(path).await
    }

    async fn mkdir(&self, path: &str, mode: u32) -> FsResult<()> {
        self.inner.mkdir(path, mode).await
    }

    async fn remove(&self, path: &str) -> FsResult<()> {
        self.inner.remove(path).await
    }

    async fn remove_all(&self, path: &str) -> FsResult<()> {
        self.inner.remove_all(path).await
    }

    async fn read(&self, path: &str, offset: u64, size: u64) -> FsResult<Vec<u8>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read(path, offset, size).await
    }

    async fn write(&self, path: &str, data: &[u8], offset: u64, flags: WriteFlag) -> FsResult<u64> {
        self.inner.write(path, data, offset, flags).await
    }

    async fn read_dir(&self, path: &str) -> FsResult<Vec<FileInfo>> {
        self.read_dirs.fetch_add(1, Ordering::SeqCst);
        self.inner.read_dir(path).await
    }

    async fn stat(&self, path: &str) -> FsResult<FileInfo> {
        self.inner.stat(path).await
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> FsResult<()> {
        self.inner.rename(old_path, new_path).await
    }

    async fn chmod(&self, path: &str, mode: u32) -> FsResult<()> {
        self.inner.chmod(path, mode).await
    }

    async fn truncate(&self, path: &str, size: u64) -> FsResult<()> {
        self.inner.truncate(path, size).await
    }

    async fn grep(
        &self,
        path: &str,
        pattern: &str,
        recursive: bool,
        case_insensitive: bool,
        node_limit: Option<usize>,
        exclude_path: Option<&str>,
        level_limit: Option<usize>,
    ) -> FsResult<GrepResult> {
        self.inner
            .grep(
                path,
                pattern,
                recursive,
                case_insensitive,
                node_limit,
                exclude_path,
                level_limit,
            )
            .await
    }

    async fn tree_directory(
        &self,
        path: &str,
        show_hidden: bool,
        node_limit: Option<usize>,
        level_limit: Option<usize>,
    ) -> FsResult<Vec<TreeEntry>> {
        self.inner
            .tree_directory(path, show_hidden, node_limit, level_limit)
            .await
    }
}

fn config() -> MemStoreConfig {
    MemStoreConfig {
        sdk_concurrency: 4,
        operation_timeout_ms: 500,
        ..MemStoreConfig::default()
    }
}

async fn cached_fs(
    backend: CountingFileSystem,
    store: Arc<SharedKvStore>,
    namespace: &str,
    policy: CachePolicy,
) -> CachedFileSystem {
    let provider: Arc<dyn CacheProvider> =
        Arc::new(MemStoreProvider::from_store(config(), store).await.unwrap());
    CachedFileSystem::new(
        Box::new(backend),
        provider,
        CacheNamespace::new(namespace),
        policy,
    )
}

#[tokio::test]
async fn miss_fill_hit_and_shorter_and_longer_overwrites_are_consistent() {
    let backend = CountingFileSystem::new();
    backend
        .write("/value.md", b"initial-value", 0, WriteFlag::Create)
        .await
        .unwrap();
    let probe = backend.clone();
    let fs = cached_fs(
        backend,
        Arc::new(SharedKvStore::default()),
        "read-write",
        CachePolicy::default(),
    )
    .await;

    assert_eq!(fs.read("/value.md", 0, 0).await.unwrap(), b"initial-value");
    assert_eq!(fs.read("/value.md", 0, 0).await.unwrap(), b"initial-value");
    assert_eq!(probe.reads.load(Ordering::SeqCst), 1);

    fs.write("/value.md", b"x", 0, WriteFlag::Truncate)
        .await
        .unwrap();
    assert_eq!(fs.read("/value.md", 0, 0).await.unwrap(), b"x");
    fs.write(
        "/value.md",
        b"a substantially longer replacement",
        0,
        WriteFlag::Truncate,
    )
    .await
    .unwrap();
    assert_eq!(
        fs.read("/value.md", 0, 0).await.unwrap(),
        b"a substantially longer replacement"
    );
    assert_eq!(probe.reads.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn remove_rename_and_remove_all_invalidate_cached_entries() {
    let backend = CountingFileSystem::new();
    backend.mkdir("/root", 0o755).await.unwrap();
    backend.mkdir("/root/tree", 0o755).await.unwrap();
    backend
        .write("/root/tree/leaf", b"old", 0, WriteFlag::Create)
        .await
        .unwrap();
    let direct = backend.clone();
    let fs = cached_fs(
        backend,
        Arc::new(SharedKvStore::default()),
        "invalidation",
        CachePolicy::default(),
    )
    .await;

    assert_eq!(fs.read("/root/tree/leaf", 0, 0).await.unwrap(), b"old");
    fs.rename("/root/tree/leaf", "/root/tree/moved")
        .await
        .unwrap();
    assert!(fs.read("/root/tree/leaf", 0, 0).await.is_err());
    assert_eq!(fs.read("/root/tree/moved", 0, 0).await.unwrap(), b"old");
    fs.remove("/root/tree/moved").await.unwrap();
    assert!(fs.read("/root/tree/moved", 0, 0).await.is_err());

    direct
        .write("/root/tree/leaf", b"stale", 0, WriteFlag::Create)
        .await
        .unwrap();
    assert_eq!(fs.read("/root/tree/leaf", 0, 0).await.unwrap(), b"stale");
    fs.remove_all("/root/tree").await.unwrap();
    direct.mkdir("/root/tree", 0o755).await.unwrap();
    direct
        .write("/root/tree/leaf", b"fresh", 0, WriteFlag::Create)
        .await
        .unwrap();
    assert_eq!(fs.read("/root/tree/leaf", 0, 0).await.unwrap(), b"fresh");

    fs.rename("/root/tree", "/root/renamed").await.unwrap();
    assert!(fs.read("/root/tree/leaf", 0, 0).await.is_err());
    assert_eq!(fs.read("/root/renamed/leaf", 0, 0).await.unwrap(), b"fresh");
}

#[tokio::test]
async fn unavailable_memstore_falls_back_without_breaking_backend_reads() {
    let backend = CountingFileSystem::new();
    backend
        .write("/available.md", b"backend", 0, WriteFlag::Create)
        .await
        .unwrap();
    let probe = backend.clone();
    let store = Arc::new(SharedKvStore::default());
    let fs = cached_fs(backend, store.clone(), "fallback", CachePolicy::default()).await;
    store.unavailable.store(true, Ordering::SeqCst);

    assert_eq!(fs.read("/available.md", 0, 0).await.unwrap(), b"backend");
    assert_eq!(fs.read("/available.md", 0, 0).await.unwrap(), b"backend");
    assert_eq!(probe.reads.load(Ordering::SeqCst), 2);
    assert!(fs.metrics().snapshot().errors >= 1);
}

#[tokio::test]
async fn warm_cached_grep_uses_memstore_batch_get() {
    let backend = CountingFileSystem::new();
    backend.mkdir("/docs", 0o755).await.unwrap();
    for index in 0..8 {
        backend
            .write(
                &format!("/docs/{index}.md"),
                b"needle\nplain",
                0,
                WriteFlag::Create,
            )
            .await
            .unwrap();
    }
    let store = Arc::new(SharedKvStore::default());
    let fs = cached_fs(
        backend,
        store.clone(),
        "batch-get",
        CachePolicy::default().with_traversal_mode(CacheTraversalMode::CachedTraversal),
    )
    .await;

    fs.grep("/docs", "needle", true, false, None, None, None)
        .await
        .unwrap();
    let before = store.batch_get_calls.load(Ordering::SeqCst);
    let result = fs
        .grep("/docs", "needle", true, false, None, None, None)
        .await
        .unwrap();

    assert_eq!(result.count, 8);
    assert!(store.batch_get_calls.load(Ordering::SeqCst) > before);
}
