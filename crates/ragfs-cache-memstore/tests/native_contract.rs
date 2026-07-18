#![cfg(feature = "memstore-native")]

use bytes::Bytes;
use ragfs::cache::{CacheError, CacheProvider};
use ragfs_cache_memstore::{MemStoreConfig, MemStoreProvider};
use ragfs_cache_memstore_sys::{
    DeleteItems, GetItems, MmsOptions, ReplaceItems, ServiceCallback, RET_MMS_BUSY, RET_MMS_EPERM,
    RET_MMS_ERROR, RET_MMS_NEED_RETRY, RET_MMS_NOT_FOUND, RET_MMS_NOT_READY, RET_MMS_OK,
    RET_MMS_READ_EXCEED,
};
use std::collections::HashMap;
use std::ffi::{c_char, c_uint};
use std::slice;
use std::sync::{Mutex, OnceLock};

const HEALTH_KEY: &str = "ovms:health:reserved-never-written:v1";
const GROWING_KEY: &str = "growing";
const SENTINEL_KEY: &str = "sentinel";
const RETRY_MISSING_KEY: &str = "retry-missing";
const RETRY_PRESENT_KEY: &str = "retry-present";
const ZERO_COPY_KEY: &str = "zero-copy";
const RESIZE_EXHAUSTION_KEY: &str = "resize-exhaustion";
const UNSET_OK_KEY: &str = "unset-overall-ok";
const ITEM_EPERM_KEY: &str = "item-eperm";
const ITEM_ERROR_KEY: &str = "item-error";
const SMALL_KEY: &str = "small";
const LARGE_KEY: &str = "large";
const EMPTY_KEY: &str = "empty";
const TRUNCATED_KEY: &str = "truncated";
const INITIAL_READ_BUFFER_SIZE: usize = 4 * 1024;
const MAX_READ_BATCH_SIZE: usize = 256;
const SMALL_PAYLOAD_SIZE: usize = 1024;
const LARGE_PAYLOAD_SIZE: usize = 5000;
const GROWN_PAYLOAD_SIZE: usize = 5 * 1024;
const ZERO_COPY_FRAME: &[u8] = b"OVMS\x01\x00\x00\x00\x01z";
const TRUNCATED_FRAME: &[u8] = b"OVMS\x01\x00\x00\x00\x04data";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedOptions {
    net_connect_count: u16,
    net_group_count: u16,
    busy_polling: u8,
    tls_enabled: u8,
    tls_paths: Vec<String>,
}

#[derive(Default)]
struct FakeMemStore {
    values: HashMap<String, Vec<u8>>,
    growing_initial_read: bool,
    get_calls: Vec<Vec<(String, usize)>>,
    initialize_calls: usize,
    successful_initializations: usize,
    exit_calls: usize,
    next_initialize_result: Option<i32>,
    observed_options: Vec<ObservedOptions>,
}

fn fake_store() -> &'static Mutex<FakeMemStore> {
    static STORE: OnceLock<Mutex<FakeMemStore>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(FakeMemStore::default()))
}

fn take_get_calls() -> Vec<Vec<(String, usize)>> {
    std::mem::take(&mut fake_store().lock().unwrap().get_calls)
}

unsafe fn key_from_raw(key: *const c_char, key_len: u16) -> String {
    String::from_utf8(slice::from_raw_parts(key.cast::<u8>(), key_len as usize).to_vec()).unwrap()
}

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(9 + payload.len());
    framed.extend_from_slice(b"OVMS\x01");
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(payload);
    framed
}

fn options_from_raw(options: &MmsOptions) -> ObservedOptions {
    let tls_paths = [
        &options.certification_path,
        &options.ca_cer_path,
        &options.ca_crl_path,
        &options.private_key_path,
        &options.private_key_password_path,
        &options.decrypter_lib_path,
        &options.openssl_lib_dir,
    ]
    .into_iter()
    .map(|path| {
        let bytes = path
            .iter()
            .take_while(|byte| **byte != 0)
            .map(|byte| *byte as u8)
            .collect::<Vec<_>>();
        String::from_utf8(bytes).unwrap()
    })
    .collect();
    ObservedOptions {
        net_connect_count: options.net_connect_cnt,
        net_group_count: options.net_group_num,
        busy_polling: options.net_is_busy_polling,
        tls_enabled: options.tls_enable,
        tls_paths,
    }
}

#[no_mangle]
unsafe extern "C" fn MmsInitialize(options: *const MmsOptions, service: ServiceCallback) -> i32 {
    let mut store = fake_store().lock().unwrap();
    store.initialize_calls += 1;
    store.observed_options.push(options_from_raw(&*options));
    if let Some(result) = store.next_initialize_result.take() {
        return result;
    }
    store.successful_initializations += 1;
    drop(store);
    if let Some(callback) = service {
        callback(1);
    }
    RET_MMS_OK
}

#[no_mangle]
unsafe extern "C" fn MmsExit() {
    fake_store().lock().unwrap().exit_calls += 1;
}

#[no_mangle]
unsafe extern "C" fn MmsReplace(items: *mut ReplaceItems, item_num: c_uint) -> i32 {
    let mut store = fake_store().lock().unwrap();
    for item in slice::from_raw_parts_mut(items, item_num as usize) {
        let key = key_from_raw(item.key, item.key_len);
        let value = slice::from_raw_parts(item.value.cast::<u8>(), item.value_len as usize);
        match store.values.get_mut(&key) {
            Some(existing) if existing.len() > value.len() => {
                existing[..value.len()].copy_from_slice(value)
            }
            Some(existing) => {
                existing.clear();
                existing.extend_from_slice(value);
            }
            None => {
                store.values.insert(key, value.to_vec());
            }
        }
        *item.result = RET_MMS_OK;
    }
    RET_MMS_OK
}

#[no_mangle]
unsafe extern "C" fn MmsGet(items: *mut GetItems, item_num: c_uint) -> i32 {
    let mut store = fake_store().lock().unwrap();
    let items = slice::from_raw_parts_mut(items, item_num as usize);
    store.get_calls.push(
        items
            .iter()
            .map(|item| (key_from_raw(item.key, item.key_len), item.length as usize))
            .collect(),
    );
    if items.len() == 1 {
        let key = key_from_raw(items[0].key, items[0].key_len);
        if key == SENTINEL_KEY {
            return RET_MMS_BUSY;
        }
    }

    for item in items {
        let key = key_from_raw(item.key, item.key_len);
        if key == ZERO_COPY_KEY {
            *item.value = ZERO_COPY_FRAME.as_ptr().cast::<c_char>().cast_mut();
            *item.real_length = ZERO_COPY_FRAME.len() as c_uint;
            *item.result = RET_MMS_READ_EXCEED;
            continue;
        }
        if key == TRUNCATED_KEY {
            std::ptr::copy_nonoverlapping(
                TRUNCATED_FRAME.as_ptr(),
                (*item.value).cast::<u8>(),
                TRUNCATED_FRAME.len(),
            );
            *item.real_length = 9;
            *item.result = RET_MMS_OK;
            continue;
        }
        if key == RESIZE_EXHAUSTION_KEY {
            let announced_payload = item.length as usize;
            let header = frame(&vec![0; announced_payload]);
            std::ptr::copy_nonoverlapping(header.as_ptr(), (*item.value).cast::<u8>(), 9);
            *item.real_length = header.len() as c_uint;
            *item.result = RET_MMS_READ_EXCEED;
            continue;
        }
        if key == UNSET_OK_KEY {
            continue;
        }
        if key == ITEM_EPERM_KEY {
            *item.result = RET_MMS_EPERM;
            continue;
        }
        if key == ITEM_ERROR_KEY {
            *item.result = RET_MMS_ERROR;
            continue;
        }
        let Some(value) = store.values.get(&key).cloned() else {
            *item.real_length = 0;
            *item.result = RET_MMS_NOT_FOUND;
            continue;
        };
        if key == GROWING_KEY
            && !store.growing_initial_read
            && item.length as usize == INITIAL_READ_BUFFER_SIZE
        {
            store
                .values
                .insert(key.clone(), frame(&vec![b'g'; GROWN_PAYLOAD_SIZE]));
            store.growing_initial_read = true;
        }
        let copied = usize::min(item.length as usize, value.len());
        std::ptr::copy_nonoverlapping(value.as_ptr(), (*item.value).cast::<u8>(), copied);
        *item.real_length = value.len() as c_uint;
        *item.result = if copied < value.len() {
            RET_MMS_READ_EXCEED
        } else {
            RET_MMS_OK
        };
    }
    RET_MMS_OK
}

#[no_mangle]
unsafe extern "C" fn MmsDelete(items: *mut DeleteItems, item_num: c_uint) -> i32 {
    let mut store = fake_store().lock().unwrap();
    for item in slice::from_raw_parts_mut(items, item_num as usize) {
        let key = key_from_raw(item.key, item.key_len);
        if key == RETRY_MISSING_KEY {
            store.values.remove(&key);
            *item.result = RET_MMS_NEED_RETRY;
        } else if key == RETRY_PRESENT_KEY {
            *item.result = RET_MMS_NEED_RETRY;
        } else {
            *item.result = if store.values.remove(&key).is_some() {
                RET_MMS_OK
            } else {
                RET_MMS_NOT_FOUND
            };
        }
    }
    RET_MMS_OK
}

fn config() -> MemStoreConfig {
    MemStoreConfig {
        sdk_concurrency: 2,
        operation_timeout_ms: 500,
        max_value_size_bytes: 8 * 1_024,
        ..MemStoreConfig::default()
    }
}

#[tokio::test]
async fn native_reads_resize_delete_retries_sentinels_and_runtime_leases_work() {
    let mut state = fake_store().lock().unwrap();
    *state = FakeMemStore::default();
    state.next_initialize_result = Some(RET_MMS_NOT_READY);
    state.values.insert(
        GROWING_KEY.into(),
        frame(&vec![b'i'; INITIAL_READ_BUFFER_SIZE]),
    );
    drop(state);

    let mut translated = config();
    translated.net_connect_count = 7;
    translated.net_group_count = 3;
    translated.busy_polling = false;
    translated.tls_enabled = true;
    translated.certification_path = "/tls/cert.pem".into();
    translated.ca_cert_path = "/tls/ca.pem".into();
    translated.ca_crl_path = "/tls/ca.crl".into();
    translated.private_key_path = "/tls/key.pem".into();
    translated.private_key_password_path = "/tls/password".into();
    translated.decrypter_lib_path = "/tls/decrypter.so".into();
    translated.openssl_lib_dir = "/tls/lib".into();
    assert!(matches!(
        MemStoreProvider::connect(translated).await.unwrap_err(),
        CacheError::Unavailable(_)
    ));
    let first = MemStoreProvider::connect(config()).await.unwrap();
    let second = MemStoreProvider::connect(config()).await.unwrap();
    {
        let state = fake_store().lock().unwrap();
        assert_eq!(state.initialize_calls, 2);
        assert_eq!(state.successful_initializations, 1);
        assert_eq!(
            state.observed_options,
            vec![
                ObservedOptions {
                    net_connect_count: 7,
                    net_group_count: 3,
                    busy_polling: 0,
                    tls_enabled: 1,
                    tls_paths: vec![
                        "/tls/cert.pem".into(),
                        "/tls/ca.pem".into(),
                        "/tls/ca.crl".into(),
                        "/tls/key.pem".into(),
                        "/tls/password".into(),
                        "/tls/decrypter.so".into(),
                        "/tls/lib".into(),
                    ],
                },
                ObservedOptions {
                    net_connect_count: 16,
                    net_group_count: 1,
                    busy_polling: 1,
                    tls_enabled: 0,
                    tls_paths: vec![String::new(); 7],
                },
            ]
        );
    }

    first
        .put(SMALL_KEY, Bytes::from(vec![b's'; SMALL_PAYLOAD_SIZE]))
        .await
        .unwrap();
    first
        .put(LARGE_KEY, Bytes::from(vec![b'l'; LARGE_PAYLOAD_SIZE]))
        .await
        .unwrap();
    first.put(EMPTY_KEY, Bytes::new()).await.unwrap();
    take_get_calls();
    assert_eq!(
        first
            .batch_get(&[SMALL_KEY.into(), LARGE_KEY.into()])
            .await
            .unwrap(),
        vec![
            Some(Bytes::from(vec![b's'; SMALL_PAYLOAD_SIZE])),
            Some(Bytes::from(vec![b'l'; LARGE_PAYLOAD_SIZE])),
        ]
    );
    assert_eq!(
        take_get_calls(),
        vec![
            vec![
                (SMALL_KEY.into(), INITIAL_READ_BUFFER_SIZE),
                (LARGE_KEY.into(), INITIAL_READ_BUFFER_SIZE),
            ],
            vec![(LARGE_KEY.into(), 9 + LARGE_PAYLOAD_SIZE)],
        ]
    );

    assert_eq!(
        first.get(SMALL_KEY).await.unwrap(),
        Some(Bytes::from(vec![b's'; SMALL_PAYLOAD_SIZE]))
    );
    assert_eq!(
        take_get_calls(),
        vec![vec![(SMALL_KEY.into(), INITIAL_READ_BUFFER_SIZE)]]
    );
    assert_eq!(first.get(EMPTY_KEY).await.unwrap(), Some(Bytes::new()));
    assert_eq!(
        take_get_calls(),
        vec![vec![(EMPTY_KEY.into(), INITIAL_READ_BUFFER_SIZE)]]
    );

    let chunk_keys = (0..=MAX_READ_BATCH_SIZE)
        .map(|index| format!("chunk-{index:03}"))
        .collect::<Vec<_>>();
    {
        let mut state = fake_store().lock().unwrap();
        for key in &chunk_keys {
            state.values.insert(key.clone(), frame(b"c"));
        }
    }
    take_get_calls();
    let chunk_results = first.batch_get(&chunk_keys).await.unwrap();
    assert_eq!(
        chunk_results,
        vec![Some(Bytes::from_static(b"c")); chunk_keys.len()]
    );
    let chunk_calls = take_get_calls();
    assert_eq!(chunk_calls.len(), 2);
    assert_eq!(chunk_calls[0].len(), MAX_READ_BATCH_SIZE);
    assert!(chunk_calls[0]
        .iter()
        .all(|(_, buffer_size)| *buffer_size == INITIAL_READ_BUFFER_SIZE));
    assert_eq!(
        chunk_calls[1],
        vec![(
            chunk_keys[MAX_READ_BATCH_SIZE].clone(),
            INITIAL_READ_BUFFER_SIZE,
        )]
    );

    first
        .put("shorter", Bytes::from(vec![b'o'; LARGE_PAYLOAD_SIZE]))
        .await
        .unwrap();
    first
        .put("shorter", Bytes::from_static(b"new"))
        .await
        .unwrap();
    assert!(first.exists("shorter").await.unwrap());
    take_get_calls();
    assert_eq!(
        first.get("shorter").await.unwrap(),
        Some(Bytes::from_static(b"new"))
    );
    assert_eq!(
        take_get_calls(),
        vec![vec![("shorter".into(), INITIAL_READ_BUFFER_SIZE)]]
    );

    first
        .put(RETRY_MISSING_KEY, Bytes::from_static(b"gone"))
        .await
        .unwrap();
    first
        .put(RETRY_PRESENT_KEY, Bytes::from_static(b"still here"))
        .await
        .unwrap();
    first.delete(RETRY_MISSING_KEY).await.unwrap();
    assert!(matches!(
        first.delete(RETRY_PRESENT_KEY).await.unwrap_err(),
        CacheError::Unavailable(_)
    ));

    let keys = vec!["shorter".into(), GROWING_KEY.into(), "missing".into()];
    take_get_calls();
    assert_eq!(
        first.batch_get(&keys).await.unwrap(),
        vec![
            Some(Bytes::from_static(b"new")),
            Some(Bytes::from(vec![b'g'; GROWN_PAYLOAD_SIZE])),
            None,
        ]
    );
    assert_eq!(
        take_get_calls(),
        vec![
            vec![
                ("shorter".into(), INITIAL_READ_BUFFER_SIZE),
                (GROWING_KEY.into(), INITIAL_READ_BUFFER_SIZE),
                ("missing".into(), INITIAL_READ_BUFFER_SIZE),
            ],
            vec![(GROWING_KEY.into(), 9 + INITIAL_READ_BUFFER_SIZE)],
            vec![(GROWING_KEY.into(), 9 + GROWN_PAYLOAD_SIZE)],
        ]
    );
    assert!(matches!(
        first.get(SENTINEL_KEY).await.unwrap_err(),
        CacheError::Unavailable(_)
    ));
    assert!(matches!(
        first.get(UNSET_OK_KEY).await.unwrap_err(),
        CacheError::Internal(_)
    ));
    assert!(matches!(
        first.get(ZERO_COPY_KEY).await.unwrap_err(),
        CacheError::Internal(_)
    ));
    assert!(matches!(
        first.get(TRUNCATED_KEY).await.unwrap_err(),
        CacheError::InvalidData(_)
    ));
    take_get_calls();
    assert!(matches!(
        first.get(RESIZE_EXHAUSTION_KEY).await.unwrap_err(),
        CacheError::Unavailable(_)
    ));
    assert_eq!(
        take_get_calls(),
        vec![
            vec![(RESIZE_EXHAUSTION_KEY.into(), 4096)],
            vec![(RESIZE_EXHAUSTION_KEY.into(), 4105)],
            vec![(RESIZE_EXHAUSTION_KEY.into(), 4114)],
            vec![(RESIZE_EXHAUSTION_KEY.into(), 4123)],
        ]
    );
    assert!(matches!(
        first.get(ITEM_EPERM_KEY).await.unwrap_err(),
        CacheError::InvalidArgument(_)
    ));
    assert!(matches!(
        first.get(ITEM_ERROR_KEY).await.unwrap_err(),
        CacheError::Internal(_)
    ));

    let mut different = config();
    different.net_connect_count += 1;
    assert!(matches!(
        MemStoreProvider::connect(different).await.unwrap_err(),
        CacheError::Unavailable(_)
    ));
    first.close().await.unwrap();
    assert_eq!(fake_store().lock().unwrap().exit_calls, 0);
    assert_eq!(
        second.get("shorter").await.unwrap(),
        Some(Bytes::from_static(b"new"))
    );
    second.close().await.unwrap();
    assert_eq!(fake_store().lock().unwrap().exit_calls, 1);
    assert!(matches!(
        MemStoreProvider::connect(config()).await.unwrap_err(),
        CacheError::Unavailable(_)
    ));

    let state = fake_store().lock().unwrap();
    assert_eq!(state.initialize_calls, 2);
    assert_eq!(state.successful_initializations, 1);
    assert_eq!(state.exit_calls, 1);
    assert!(!state.values.contains_key(HEALTH_KEY));
}
