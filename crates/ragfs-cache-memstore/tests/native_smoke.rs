#![cfg(feature = "memstore-native")]

use bytes::Bytes;
use ragfs::cache::CacheProvider;
use ragfs_cache_memstore::{MemStoreConfig, MemStoreProvider};
use std::panic::resume_unwind;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn native_memstore_round_trips_values_batches_deletes_and_close() {
    if std::env::var("OPENVIKING_RUN_MEMSTORE_INTEGRATION").as_deref() != Ok("true") {
        return;
    }

    let mut config = MemStoreConfig::default();
    if let Ok(value) = std::env::var("MEMSTORE_OPERATION_TIMEOUT_MS") {
        config.operation_timeout_ms = value
            .parse()
            .expect("MEMSTORE_OPERATION_TIMEOUT_MS must be a positive integer");
    }
    if let Ok(value) = std::env::var("MEMSTORE_SDK_CONCURRENCY") {
        config.sdk_concurrency = value
            .parse()
            .expect("MEMSTORE_SDK_CONCURRENCY must be a positive integer");
    }
    if let Ok(value) = std::env::var("MEMSTORE_NET_GROUP_COUNT") {
        config.net_group_count = value
            .parse()
            .expect("MEMSTORE_NET_GROUP_COUNT must be a positive integer");
    }

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!(
        "openviking:memstore:smoke:{}:{timestamp}",
        std::process::id()
    );
    let normal = format!("{prefix}:normal");
    let empty = format!("{prefix}:empty");
    let long_to_short = format!("{prefix}:long-to-short");
    let short_to_long = format!("{prefix}:short-to-long");
    let batch_first = format!("{prefix}:batch-first");
    let batch_second = format!("{prefix}:batch-second");
    let missing = format!("{prefix}:missing");
    let cleanup_keys = vec![
        normal.clone(),
        empty.clone(),
        long_to_short.clone(),
        short_to_long.clone(),
        batch_first.clone(),
        batch_second.clone(),
        missing.clone(),
    ];

    let provider = Arc::new(MemStoreProvider::connect(config).await.unwrap());
    let operations_provider = Arc::clone(&provider);
    let operations = tokio::spawn(async move {
        operations_provider.health_check().await.unwrap();

        assert_eq!(operations_provider.get(&normal).await.unwrap(), None);

        operations_provider
            .put(&normal, Bytes::from_static(b"normal-value"))
            .await
            .unwrap();
        assert_eq!(
            operations_provider.get(&normal).await.unwrap(),
            Some(Bytes::from_static(b"normal-value"))
        );

        operations_provider.put(&empty, Bytes::new()).await.unwrap();
        assert_eq!(
            operations_provider.get(&empty).await.unwrap(),
            Some(Bytes::new())
        );

        operations_provider
            .put(
                &long_to_short,
                Bytes::from_static(b"a much longer value before the short overwrite"),
            )
            .await
            .unwrap();
        operations_provider
            .put(&long_to_short, Bytes::from_static(b"short"))
            .await
            .unwrap();
        assert_eq!(
            operations_provider.get(&long_to_short).await.unwrap(),
            Some(Bytes::from_static(b"short"))
        );

        operations_provider
            .put(&short_to_long, Bytes::from_static(b"short"))
            .await
            .unwrap();
        operations_provider
            .put(
                &short_to_long,
                Bytes::from_static(b"a much longer value after the short original"),
            )
            .await
            .unwrap();
        assert_eq!(
            operations_provider.get(&short_to_long).await.unwrap(),
            Some(Bytes::from_static(
                b"a much longer value after the short original"
            ))
        );

        operations_provider
            .batch_put(vec![
                (
                    batch_first.clone(),
                    Bytes::from_static(b"batch-first-value"),
                ),
                (
                    batch_second.clone(),
                    Bytes::from_static(b"batch-second-value"),
                ),
            ])
            .await
            .unwrap();
        assert_eq!(
            operations_provider
                .batch_get(&[batch_second.clone(), missing.clone(), batch_first.clone()])
                .await
                .unwrap(),
            vec![
                Some(Bytes::from_static(b"batch-second-value")),
                None,
                Some(Bytes::from_static(b"batch-first-value")),
            ]
        );

        operations_provider
            .invalidate(&[normal.clone(), missing.clone(), empty.clone()])
            .await
            .unwrap();
        operations_provider.delete(&long_to_short).await.unwrap();
        operations_provider.delete(&long_to_short).await.unwrap();
        operations_provider
            .invalidate(&[
                short_to_long.clone(),
                batch_first.clone(),
                batch_second.clone(),
            ])
            .await
            .unwrap();

        let final_keys = vec![
            normal,
            empty,
            long_to_short,
            short_to_long,
            batch_first,
            batch_second,
            missing,
        ];
        assert_eq!(
            operations_provider.batch_get(&final_keys).await.unwrap(),
            vec![None; final_keys.len()]
        );
    })
    .await;

    let cleanup_result = provider.invalidate(&cleanup_keys).await;
    let close_result = provider.close().await;

    if let Err(join_error) = operations {
        if let Err(error) = &cleanup_result {
            eprintln!("MemStore smoke cleanup failed after operation failure: {error}");
        }
        if let Err(error) = &close_result {
            eprintln!("MemStore smoke close failed after operation failure: {error}");
        }
        if join_error.is_panic() {
            resume_unwind(join_error.into_panic());
        }
        panic!("MemStore smoke operation task failed: {join_error}");
    }

    cleanup_result.unwrap();
    close_result.unwrap();
}
