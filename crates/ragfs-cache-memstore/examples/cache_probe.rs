use ragfs::cache::CacheProvider;
use ragfs_cache_memstore::{MemStoreConfig, MemStoreProvider};

fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn cache_key(namespace: &str, kind: &str, path: &str) -> String {
    let normalized = if path == "/" {
        "/".to_string()
    } else {
        format!("/{}", path.trim_matches('/'))
    };
    format!(
        "ragfs:v2:{namespace}:{kind}:{:016x}",
        stable_hash(normalized.as_bytes())
    )
}

#[tokio::main]
async fn main() {
    let namespace = std::env::args()
        .nth(1)
        .expect("namespace argument required");
    let provider = MemStoreProvider::connect(MemStoreConfig::default())
        .await
        .expect("connect MemStore");
    provider
        .health_check()
        .await
        .expect("MemStore health check");

    let suffix = "resources/ub_supernode/v1/payload/size_1k/cohort_r0/owner_74/group_00/shard_00000/file_00000000.txt";
    let candidates = [
        format!("/default/{suffix}"),
        format!("/{suffix}"),
        format!("/local/default/{suffix}"),
    ];
    let mut hits = 0;
    for path in candidates {
        let key = cache_key(&namespace, "file", &path);
        match provider.get(&key).await {
            Ok(Some(value)) => {
                hits += 1;
                println!("hit path={path} key={key} bytes={}", value.len());
            }
            Ok(None) => println!("miss path={path} key={key}"),
            Err(error) => println!("error path={path} key={key} error={error}"),
        }
    }
    provider.close().await.expect("close MemStore");
    assert!(hits > 0, "no expected OpenViking file cache key was found");
}
