use crate::client::map_store_error;
use crate::frame::{decode_owned_value, encode_value, native_key, payload_len, HEADER_LEN};
use crate::read_buffer::ReadBufferPool;
use crate::{
    MemStoreConfig, MemStoreItemResult, MemStoreKvStore, MemStoreProvider, MemStoreStoreError,
};
use bytes::Bytes;
use ragfs::cache::{CacheError, CacheResult};
use ragfs_cache_memstore_sys as sys;
use std::cell::RefCell;
use std::ffi::{c_char, c_uint};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

const RESULT_SENTINEL: i32 = i32::MIN;
const HEALTH_KEY: &str = "ovms:health:reserved-never-written:v1";
const INITIAL_READ_BUFFER_SIZE: usize = 4 * 1024;
const DIRECTORY_READ_BUFFER_SIZE: usize = 32 * 1024;
const MAX_READ_BATCH_SIZE: usize = 256;
const MAX_RESIZE_ATTEMPTS: usize = 3;
const MAX_POOLED_READ_BUFFER_CAPACITY: usize = MAX_READ_BATCH_SIZE * DIRECTORY_READ_BUFFER_SIZE;
const MAX_POOLED_READ_TOTAL_CAPACITY: usize = 64 * 1024 * 1024;

static SERVICEABLE: AtomicBool = AtomicBool::new(false);
static RUNTIME: OnceLock<Mutex<RuntimeState>> = OnceLock::new();

#[derive(Debug, Clone, PartialEq, Eq)]
struct InitFingerprint {
    net_connect_count: u16,
    net_group_count: u16,
    busy_polling: bool,
    tls_enabled: bool,
    certification_path: String,
    ca_cert_path: String,
    ca_crl_path: String,
    private_key_path: String,
    private_key_password_path: String,
    decrypter_lib_path: String,
    openssl_lib_dir: String,
}

impl From<&MemStoreConfig> for InitFingerprint {
    fn from(config: &MemStoreConfig) -> Self {
        Self {
            net_connect_count: config.net_connect_count,
            net_group_count: config.net_group_count,
            busy_polling: config.busy_polling,
            tls_enabled: config.tls_enabled,
            certification_path: config.certification_path.clone(),
            ca_cert_path: config.ca_cert_path.clone(),
            ca_crl_path: config.ca_crl_path.clone(),
            private_key_path: config.private_key_path.clone(),
            private_key_password_path: config.private_key_password_path.clone(),
            decrypter_lib_path: config.decrypter_lib_path.clone(),
            openssl_lib_dir: config.openssl_lib_dir.clone(),
        }
    }
}

enum RuntimeState {
    Uninitialized,
    Active {
        fingerprint: InitFingerprint,
        leases: usize,
    },
    Closed,
}

struct NativeMemStore {
    max_value_size: usize,
    read_buffers: Arc<ReadBufferPool>,
    released: AtomicBool,
}

struct RawRead {
    buffer: Bytes,
    caller_buffer_preserved: bool,
    real_length: usize,
    result: i32,
}

struct PendingRead {
    index: usize,
    buffer_size: usize,
    resize_attempts: usize,
}

#[derive(Default)]
struct RawGetScratch {
    offsets: Vec<usize>,
    returned_ptrs: Vec<*mut c_char>,
    real_lengths: Vec<u32>,
    results: Vec<i32>,
    items: Vec<sys::GetItems>,
}

thread_local! {
    static RAW_GET_SCRATCH: RefCell<RawGetScratch> = RefCell::new(RawGetScratch::default());
}

impl RawGetScratch {
    fn prepare(
        &mut self,
        keys: &[String],
        buffer_sizes: &[usize],
        buffer: &mut [u8],
    ) -> Result<(), MemStoreStoreError> {
        self.clear();
        self.offsets.reserve(keys.len());
        self.returned_ptrs.reserve(keys.len());
        self.real_lengths.resize(keys.len(), 0);
        self.results.resize(keys.len(), RESULT_SENTINEL);
        self.items.reserve(keys.len());

        let buffer_base = buffer.as_mut_ptr();
        let mut offset = 0usize;
        for size in buffer_sizes {
            u32::try_from(*size).map_err(|_| {
                MemStoreStoreError::InvalidArgument(
                    "MemStore read buffer size exceeds c_uint::MAX".into(),
                )
            })?;
            let next_offset = offset.checked_add(*size).ok_or_else(|| {
                MemStoreStoreError::InvalidArgument("MemStore read batch size overflowed".into())
            })?;
            if next_offset > buffer.len() {
                return Err(MemStoreStoreError::InvalidArgument(
                    "MemStore read buffers exceed the contiguous allocation".into(),
                ));
            }
            self.offsets.push(offset);
            let header_end = offset + HEADER_LEN.min(*size);
            buffer[offset..header_end].fill(0);
            self.returned_ptrs
                .push(unsafe { buffer_base.add(offset) }.cast::<c_char>());
            offset = next_offset;
        }

        let returned_ptrs_base = self.returned_ptrs.as_mut_ptr();
        let real_lengths_base = self.real_lengths.as_mut_ptr();
        let results_base = self.results.as_mut_ptr();
        for (index, key) in keys.iter().enumerate() {
            self.items.push(sys::GetItems {
                key: key.as_ptr().cast(),
                key_len: key.len() as u16,
                offset: 0,
                length: buffer_sizes[index] as u32,
                value: unsafe { returned_ptrs_base.add(index) },
                real_length: unsafe { real_lengths_base.add(index) },
                result: unsafe { results_base.add(index) },
            });
        }
        Ok(())
    }

    fn clear(&mut self) {
        self.items.clear();
        self.results.clear();
        self.real_lengths.clear();
        self.returned_ptrs.clear();
        self.offsets.clear();
    }
}

unsafe extern "C" fn service_callback(serviceable: u8) {
    SERVICEABLE.store(serviceable != 0, Ordering::Release);
}

impl NativeMemStore {
    fn connect(config: &MemStoreConfig) -> Result<Self, MemStoreStoreError> {
        acquire_runtime(config)?;
        Ok(Self {
            max_value_size: config.max_value_size_bytes,
            read_buffers: Arc::new(ReadBufferPool::new(
                config.sdk_concurrency.saturating_mul(2).max(1),
                MAX_POOLED_READ_TOTAL_CAPACITY,
                MAX_POOLED_READ_BUFFER_CAPACITY,
            )),
            released: AtomicBool::new(false),
        })
    }

    fn release(&self) -> Result<(), MemStoreStoreError> {
        if self.released.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        release_runtime()
    }

    fn native_keys(keys: &[String]) -> Result<Vec<String>, MemStoreStoreError> {
        keys.iter().map(|key| native_key(key)).collect()
    }

    fn probe_exists(&self, key: &str) -> Result<bool, MemStoreStoreError> {
        let key = native_key(key)?;
        let (overall, mut reads) = self.raw_get(&[key], &[HEADER_LEN])?;
        let read = reads.pop().ok_or_else(|| {
            MemStoreStoreError::Internal("MemStore existence probe returned no item".into())
        })?;
        match effective_result(read.result, overall, "existence probe") {
            Ok(sys::RET_MMS_OK | sys::RET_MMS_READ_EXCEED) if read.caller_buffer_preserved => {
                Ok(true)
            }
            Ok(sys::RET_MMS_OK | sys::RET_MMS_READ_EXCEED) => {
                Err(zero_copy_error("existence probe"))
            }
            Ok(sys::RET_MMS_NOT_FOUND | sys::RET_MMS_MISS) => Ok(false),
            Ok(code) => Err(map_native_status(code, "existence probe")),
            Err(error) => Err(error),
        }
    }

    fn raw_get(
        &self,
        keys: &[String],
        buffer_sizes: &[usize],
    ) -> Result<(i32, Vec<RawRead>), MemStoreStoreError> {
        if keys.len() != buffer_sizes.len() {
            return Err(MemStoreStoreError::Internal(
                "MemStore raw get key and buffer counts differ".into(),
            ));
        }
        if keys.len() > c_uint::MAX as usize {
            return Err(MemStoreStoreError::InvalidArgument(
                "MemStore batch size exceeds c_uint::MAX".into(),
            ));
        }
        if keys.is_empty() {
            return Ok((sys::RET_MMS_OK, Vec::new()));
        }

        let total_size = buffer_sizes.iter().try_fold(0usize, |total, size| {
            total.checked_add(*size).ok_or_else(|| {
                MemStoreStoreError::InvalidArgument("MemStore read batch size overflowed".into())
            })
        })?;
        let mut buffer = self.read_buffers.take(total_size);

        RAW_GET_SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            scratch.prepare(keys, buffer_sizes, &mut buffer)?;
            let overall =
                unsafe { sys::MmsGet(scratch.items.as_mut_ptr(), scratch.items.len() as c_uint) };
            let buffer = buffer.into_bytes();
            let reads = (0..keys.len())
                .map(|index| {
                    let start = scratch.offsets[index];
                    let end = start + buffer_sizes[index];
                    let caller_ptr = unsafe { buffer.as_ptr().add(start) }.cast::<c_char>();
                    RawRead {
                        buffer: buffer.slice(start..end),
                        caller_buffer_preserved: scratch.returned_ptrs[index]
                            == caller_ptr.cast_mut(),
                        real_length: scratch.real_lengths[index] as usize,
                        result: scratch.results[index],
                    }
                })
                .collect();
            scratch.clear();
            Ok((overall, reads))
        })
    }

    fn read_results(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<Option<Bytes>>>, MemStoreStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let native_keys = Self::native_keys(keys)?;
        let mut outcomes = std::iter::repeat_with(|| None)
            .take(keys.len())
            .collect::<Vec<Option<MemStoreItemResult<Option<Bytes>>>>>();
        let max_frame_size = HEADER_LEN + self.max_value_size;
        let mut pending = (0..native_keys.len())
            .map(|index| PendingRead {
                index,
                buffer_size: initial_read_buffer_size(&keys[index], max_frame_size),
                resize_attempts: 0,
            })
            .collect::<Vec<_>>();

        while !pending.is_empty() {
            let mut retries = Vec::new();

            for chunk in pending.chunks(MAX_READ_BATCH_SIZE) {
                let call_keys = chunk
                    .iter()
                    .map(|item| native_keys[item.index].clone())
                    .collect::<Vec<_>>();
                let call_sizes = chunk
                    .iter()
                    .map(|item| item.buffer_size)
                    .collect::<Vec<_>>();
                let (overall, reads) = self.raw_get(&call_keys, &call_sizes)?;

                for (item, read) in chunk.iter().zip(reads) {
                    match effective_result(read.result, overall, "get") {
                        Ok(sys::RET_MMS_NOT_FOUND | sys::RET_MMS_MISS) => {
                            outcomes[item.index] = Some(Ok(None));
                        }
                        Ok(sys::RET_MMS_OK | sys::RET_MMS_READ_EXCEED) => {
                            if !read.caller_buffer_preserved {
                                outcomes[item.index] = Some(Err(zero_copy_error("get")));
                                continue;
                            }
                            let current_payload =
                                match payload_len(&read.buffer, self.max_value_size) {
                                    Ok(payload_size) => payload_size,
                                    Err(error) => {
                                        outcomes[item.index] = Some(Err(error));
                                        continue;
                                    }
                                };
                            let required_size = HEADER_LEN + current_payload;
                            if read.real_length < required_size {
                                outcomes[item.index] = Some(Err(MemStoreStoreError::InvalidData(
                                    "MemStore get returned a truncated frame".into(),
                                )));
                            } else if required_size > item.buffer_size {
                                if item.resize_attempts < MAX_RESIZE_ATTEMPTS {
                                    retries.push(PendingRead {
                                        index: item.index,
                                        buffer_size: required_size,
                                        resize_attempts: item.resize_attempts + 1,
                                    });
                                } else {
                                    outcomes[item.index] =
                                        Some(Err(MemStoreStoreError::Unavailable(
                                            "MemStore value kept growing during 3 resize attempts"
                                                .into(),
                                        )));
                                }
                            } else {
                                outcomes[item.index] = Some(
                                    decode_owned_value(read.buffer, self.max_value_size).map(Some),
                                );
                            }
                        }
                        Ok(code) => {
                            outcomes[item.index] = Some(Err(map_native_status(code, "get")));
                        }
                        Err(error) => outcomes[item.index] = Some(Err(error)),
                    }
                }
            }
            pending = retries;
        }

        Ok(outcomes
            .into_iter()
            .map(|outcome| {
                outcome.unwrap_or_else(|| {
                    Err(MemStoreStoreError::Internal(
                        "MemStore get did not produce an item result".into(),
                    ))
                })
            })
            .collect())
    }

    fn replace_results(
        &self,
        entries: &[(String, Vec<u8>)],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if entries.len() > c_uint::MAX as usize {
            return Err(MemStoreStoreError::InvalidArgument(
                "MemStore batch size exceeds c_uint::MAX".into(),
            ));
        }
        let keys = entries
            .iter()
            .map(|(key, _)| native_key(key))
            .collect::<Result<Vec<_>, _>>()?;
        let values = entries
            .iter()
            .map(|(_, value)| encode_value(value, self.max_value_size))
            .collect::<Result<Vec<_>, _>>()?;
        let mut results = vec![RESULT_SENTINEL; entries.len()];
        let results_base = results.as_mut_ptr();
        let mut items = keys
            .iter()
            .zip(&values)
            .enumerate()
            .map(|(index, (key, value))| sys::ReplaceItems {
                key: key.as_ptr().cast(),
                value: value.as_ptr().cast(),
                key_len: key.len() as u16,
                value_len: value.len() as c_uint,
                offset: 0,
                result: unsafe { results_base.add(index) },
            })
            .collect::<Vec<_>>();
        let overall = unsafe { sys::MmsReplace(items.as_mut_ptr(), items.len() as c_uint) };

        Ok(results
            .into_iter()
            .map(
                |result| match effective_result(result, overall, "replace") {
                    Ok(sys::RET_MMS_OK) => Ok(()),
                    Ok(code) => Err(map_native_status(code, "replace")),
                    Err(error) => Err(error),
                },
            )
            .collect())
    }

    fn delete_results(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if keys.len() > c_uint::MAX as usize {
            return Err(MemStoreStoreError::InvalidArgument(
                "MemStore batch size exceeds c_uint::MAX".into(),
            ));
        }
        let native_keys = Self::native_keys(keys)?;
        let mut results = vec![RESULT_SENTINEL; keys.len()];
        let results_base = results.as_mut_ptr();
        let mut items = native_keys
            .iter()
            .enumerate()
            .map(|(index, key)| sys::DeleteItems {
                key: key.as_ptr().cast(),
                key_len: key.len() as u16,
                is_notify: 0,
                result: unsafe { results_base.add(index) },
            })
            .collect::<Vec<_>>();
        let overall = unsafe { sys::MmsDelete(items.as_mut_ptr(), items.len() as c_uint) };

        Ok(results
            .into_iter()
            .enumerate()
            .map(
                |(index, result)| match effective_result(result, overall, "delete") {
                    Ok(sys::RET_MMS_OK | sys::RET_MMS_NOT_FOUND | sys::RET_MMS_MISS) => Ok(()),
                    Ok(sys::RET_MMS_NEED_RETRY) => match self.probe_exists(&keys[index]) {
                        Ok(false) => Ok(()),
                        Ok(true) => Err(MemStoreStoreError::Unavailable(
                            "MemStore delete needs retry and the key still exists".into(),
                        )),
                        Err(error) => Err(error),
                    },
                    Ok(code) => Err(map_native_status(code, "delete")),
                    Err(error) => Err(error),
                },
            )
            .collect())
    }
}

impl Drop for NativeMemStore {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

impl MemStoreKvStore for NativeMemStore {
    fn health_check(&self) -> Result<(), MemStoreStoreError> {
        let key = native_key(HEALTH_KEY)?;
        let (overall, mut reads) = self.raw_get(&[key], &[HEADER_LEN])?;
        let read = reads.pop().ok_or_else(|| {
            MemStoreStoreError::Internal("MemStore health read returned no item".into())
        })?;
        match effective_result(read.result, overall, "health check") {
            Ok(sys::RET_MMS_OK | sys::RET_MMS_READ_EXCEED) if read.caller_buffer_preserved => {
                Ok(())
            }
            Ok(sys::RET_MMS_OK | sys::RET_MMS_READ_EXCEED) => Err(zero_copy_error("health check")),
            Ok(sys::RET_MMS_NOT_FOUND | sys::RET_MMS_MISS) => Ok(()),
            Ok(code) => Err(map_native_status(code, "health check")),
            Err(error) => Err(error),
        }
    }

    fn get(&self, key: &str) -> Result<Option<Bytes>, MemStoreStoreError> {
        self.read_results(&[key.to_owned()])?
            .pop()
            .ok_or_else(|| MemStoreStoreError::Internal("MemStore get returned no item".into()))?
    }

    fn set(&self, key: &str, value: &[u8]) -> Result<(), MemStoreStoreError> {
        self.replace_results(&[(key.to_owned(), value.to_vec())])?
            .pop()
            .ok_or_else(|| MemStoreStoreError::Internal("MemStore set returned no item".into()))?
    }

    fn delete(&self, key: &str) -> Result<(), MemStoreStoreError> {
        let result = self
            .delete_results(&[key.to_owned()])?
            .pop()
            .ok_or_else(|| {
                MemStoreStoreError::Internal("MemStore delete returned no item".into())
            })?;
        result
    }

    fn exists(&self, key: &str) -> Result<bool, MemStoreStoreError> {
        self.probe_exists(key)
    }

    fn batch_get(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<Option<Bytes>>>, MemStoreStoreError> {
        self.read_results(keys)
    }

    fn batch_set(
        &self,
        entries: &[(String, Vec<u8>)],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError> {
        self.replace_results(entries)
    }

    fn batch_delete(
        &self,
        keys: &[String],
    ) -> Result<Vec<MemStoreItemResult<()>>, MemStoreStoreError> {
        self.delete_results(keys)
    }

    fn shutdown(&self) -> Result<(), MemStoreStoreError> {
        self.release()
    }
}

fn initial_read_buffer_size(key: &str, max_frame_size: usize) -> usize {
    let preferred = if is_ragfs_directory_key(key) {
        DIRECTORY_READ_BUFFER_SIZE
    } else {
        INITIAL_READ_BUFFER_SIZE
    };
    preferred.min(max_frame_size)
}

fn is_ragfs_directory_key(key: &str) -> bool {
    let Some((prefix_and_kind, _hash)) = key.rsplit_once(':') else {
        return false;
    };
    let Some((prefix, kind)) = prefix_and_kind.rsplit_once(':') else {
        return false;
    };
    prefix.starts_with("ragfs:v2:") && kind == "dir"
}

impl MemStoreProvider {
    /// Connect to the process-global MemStore runtime through the C API.
    pub async fn connect(config: MemStoreConfig) -> CacheResult<Self> {
        config.validate()?;
        let setup_config = config.clone();
        let store = tokio::task::spawn_blocking(move || NativeMemStore::connect(&setup_config))
            .await
            .map_err(|error| {
                CacheError::Internal(format!("MemStore initialization task failed: {error}"))
            })?
            .map_err(map_store_error)?;

        Self::from_store(config, Arc::new(store)).await
    }
}

fn runtime() -> &'static Mutex<RuntimeState> {
    RUNTIME.get_or_init(|| Mutex::new(RuntimeState::Uninitialized))
}

fn acquire_runtime(config: &MemStoreConfig) -> Result<(), MemStoreStoreError> {
    let fingerprint = InitFingerprint::from(config);
    let mut state = runtime()
        .lock()
        .map_err(|_| MemStoreStoreError::Internal("MemStore runtime state is poisoned".into()))?;
    match &mut *state {
        RuntimeState::Uninitialized => {
            let options = build_options(config);
            SERVICEABLE.store(false, Ordering::Release);
            let code = unsafe { sys::MmsInitialize(&options, Some(service_callback)) };
            if code != sys::RET_MMS_OK {
                return Err(map_native_status(code, "initialize"));
            }
            *state = RuntimeState::Active {
                fingerprint,
                leases: 1,
            };
            Ok(())
        }
        RuntimeState::Active {
            fingerprint: active,
            leases,
        } if active == &fingerprint => {
            *leases = leases.checked_add(1).ok_or_else(|| {
                MemStoreStoreError::Internal("MemStore runtime lease count overflowed".into())
            })?;
            Ok(())
        }
        RuntimeState::Active { .. } => Err(MemStoreStoreError::Unavailable(
            "MemStore runtime is already initialized with different options".into(),
        )),
        RuntimeState::Closed => Err(MemStoreStoreError::Unavailable(
            "MemStore runtime was permanently closed in this process".into(),
        )),
    }
}

fn release_runtime() -> Result<(), MemStoreStoreError> {
    let mut state = runtime()
        .lock()
        .map_err(|_| MemStoreStoreError::Internal("MemStore runtime state is poisoned".into()))?;
    match &mut *state {
        RuntimeState::Active { leases, .. } if *leases > 1 => {
            *leases -= 1;
            Ok(())
        }
        RuntimeState::Active { .. } => {
            unsafe { sys::MmsExit() };
            SERVICEABLE.store(false, Ordering::Release);
            *state = RuntimeState::Closed;
            Ok(())
        }
        RuntimeState::Uninitialized => Err(MemStoreStoreError::Internal(
            "MemStore runtime lease released before initialization".into(),
        )),
        RuntimeState::Closed => Ok(()),
    }
}

fn build_options(config: &MemStoreConfig) -> sys::MmsOptions {
    sys::MmsOptions {
        net_connect_cnt: config.net_connect_count,
        net_group_num: config.net_group_count,
        net_is_busy_polling: u8::from(config.busy_polling),
        tls_enable: u8::from(config.tls_enabled),
        certification_path: path_array(&config.certification_path),
        ca_cer_path: path_array(&config.ca_cert_path),
        ca_crl_path: path_array(&config.ca_crl_path),
        private_key_path: path_array(&config.private_key_path),
        private_key_password_path: path_array(&config.private_key_password_path),
        decrypter_lib_path: path_array(&config.decrypter_lib_path),
        openssl_lib_dir: path_array(&config.openssl_lib_dir),
    }
}

fn path_array(path: &str) -> [c_char; libc::PATH_MAX as usize] {
    let mut output = [0; libc::PATH_MAX as usize];
    for (target, source) in output.iter_mut().zip(path.as_bytes()) {
        *target = *source as c_char;
    }
    output
}

fn effective_result(
    item_result: i32,
    overall_result: i32,
    operation: &str,
) -> Result<i32, MemStoreStoreError> {
    if item_result != RESULT_SENTINEL {
        Ok(item_result)
    } else if overall_result != sys::RET_MMS_OK {
        Ok(overall_result)
    } else {
        Err(MemStoreStoreError::Internal(format!(
            "MemStore {operation} left an item result unset"
        )))
    }
}

fn zero_copy_error(operation: &str) -> MemStoreStoreError {
    MemStoreStoreError::Internal(format!(
        "MemStore {operation} replaced a caller-owned read buffer"
    ))
}

fn map_native_status(code: i32, operation: &str) -> MemStoreStoreError {
    let detail = format!(
        "{operation} returned MemStore status {code} (serviceable={})",
        SERVICEABLE.load(Ordering::Acquire)
    );
    match code {
        sys::RET_MMS_BUSY
        | sys::RET_MMS_NEED_RETRY
        | sys::RET_MMS_NOT_READY
        | sys::RET_MMS_UNAVAILABLE
        | sys::RET_MMS_NO_SPACE
        | sys::RET_MMS_EXCEED_QUOTA
        | sys::RET_MMS_PT_FAULT => MemStoreStoreError::Unavailable(detail),
        sys::RET_MMS_EPERM | sys::RET_MMS_READ_EXCEED => {
            MemStoreStoreError::InvalidArgument(detail)
        }
        sys::RET_MMS_ERROR
        | sys::RET_MMS_PROTECTED
        | sys::RET_MMS_CONFLICT
        | sys::RET_MMS_EXISTS
        | sys::RET_MMS_NOT_FOUND
        | sys::RET_MMS_MISS
        | sys::RET_MMS_OK => MemStoreStoreError::Internal(detail),
        _ => MemStoreStoreError::Internal(detail),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_get_scratch_reuses_ffi_metadata_allocations() {
        let keys = vec!["first".to_owned(), "second".to_owned()];
        let buffer_sizes = vec![INITIAL_READ_BUFFER_SIZE; keys.len()];
        let mut buffer = vec![0x5a_u8; buffer_sizes.iter().sum()];
        let mut scratch = RawGetScratch::default();

        scratch.prepare(&keys, &buffer_sizes, &mut buffer).unwrap();
        for offset in [0, INITIAL_READ_BUFFER_SIZE] {
            assert_eq!(&buffer[offset..offset + HEADER_LEN], &[0; HEADER_LEN]);
            assert!(
                buffer[offset + HEADER_LEN..offset + INITIAL_READ_BUFFER_SIZE]
                    .iter()
                    .all(|byte| *byte == 0x5a)
            );
        }
        let allocations = (
            scratch.offsets.as_ptr(),
            scratch.returned_ptrs.as_ptr(),
            scratch.real_lengths.as_ptr(),
            scratch.results.as_ptr(),
            scratch.items.as_ptr(),
        );
        assert_eq!(scratch.offsets, vec![0, INITIAL_READ_BUFFER_SIZE]);
        assert_eq!(scratch.returned_ptrs[0].cast::<u8>(), buffer.as_mut_ptr());
        assert_eq!(scratch.returned_ptrs[1].cast::<u8>(), unsafe {
            buffer.as_mut_ptr().add(INITIAL_READ_BUFFER_SIZE)
        });

        scratch.prepare(&keys, &buffer_sizes, &mut buffer).unwrap();

        assert_eq!(
            (
                scratch.offsets.as_ptr(),
                scratch.returned_ptrs.as_ptr(),
                scratch.real_lengths.as_ptr(),
                scratch.results.as_ptr(),
                scratch.items.as_ptr(),
            ),
            allocations
        );
    }
}
