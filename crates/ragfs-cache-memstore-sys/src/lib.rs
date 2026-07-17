//! Unsafe bindings for the MemStore C ABI in `mms_c.h`.

use std::ffi::{c_char, c_int, c_uchar, c_uint, c_ushort};

/// MemStore operation result, matching the C `CResult` enum ABI.
pub type CResult = c_int;

pub const RET_MMS_OK: CResult = 0;
pub const RET_MMS_PROTECTED: CResult = 1;
pub const RET_MMS_ERROR: CResult = 2;
pub const RET_MMS_EPERM: CResult = 3;
pub const RET_MMS_BUSY: CResult = 4;
pub const RET_MMS_NEED_RETRY: CResult = 5;
pub const RET_MMS_NOT_READY: CResult = 6;
pub const RET_MMS_NOT_FOUND: CResult = 7;
pub const RET_MMS_CONFLICT: CResult = 8;
pub const RET_MMS_MISS: CResult = 9;
pub const RET_MMS_NO_SPACE: CResult = 10;
pub const RET_MMS_UNAVAILABLE: CResult = 11;
pub const RET_MMS_EXCEED_QUOTA: CResult = 12;
pub const RET_MMS_PT_FAULT: CResult = 13;
pub const RET_MMS_READ_EXCEED: CResult = 14;
pub const RET_MMS_EXISTS: CResult = 15;

/// Options passed to `MmsInitialize`.
#[repr(C)]
pub struct MmsOptions {
    pub net_connect_cnt: c_ushort,
    pub net_group_num: c_ushort,
    pub net_is_busy_polling: c_uchar,
    pub tls_enable: c_uchar,
    pub certification_path: [c_char; libc::PATH_MAX as usize],
    pub ca_cer_path: [c_char; libc::PATH_MAX as usize],
    pub ca_crl_path: [c_char; libc::PATH_MAX as usize],
    pub private_key_path: [c_char; libc::PATH_MAX as usize],
    pub private_key_password_path: [c_char; libc::PATH_MAX as usize],
    pub decrypter_lib_path: [c_char; libc::PATH_MAX as usize],
    pub openssl_lib_dir: [c_char; libc::PATH_MAX as usize],
}

/// Descriptor for `MmsPut` items.
#[repr(C)]
pub struct PutItems {
    pub key: *const c_char,
    pub value: *const c_char,
    pub value_len: c_uint,
    pub key_len: c_ushort,
    pub is_notify: c_ushort,
    pub value_addr: *mut *mut c_char,
    pub result: *mut i32,
}

/// Descriptor for `MmsGet` items.
#[repr(C)]
pub struct GetItems {
    pub key: *const c_char,
    pub key_len: c_ushort,
    pub offset: c_uint,
    pub length: c_uint,
    pub value: *mut *mut c_char,
    pub real_length: *mut c_uint,
    pub result: *mut i32,
}

/// Descriptor for `MmsUpdate` items.
#[repr(C)]
pub struct UpdateItems {
    pub key: *const c_char,
    pub value: *const c_char,
    pub key_len: c_ushort,
    pub value_len: c_uint,
    pub offset: c_uint,
    pub result: *mut i32,
}

/// `ReplaceItems` is the same C type as `UpdateItems`.
pub type ReplaceItems = UpdateItems;

/// Descriptor for `MmsDelete` items.
#[repr(C)]
pub struct DeleteItems {
    pub key: *const c_char,
    pub key_len: c_ushort,
    pub is_notify: c_ushort,
    pub result: *mut i32,
}

/// Callback receiving MemStore service availability updates.
pub type ServiceCallback = Option<unsafe extern "C" fn(serviceable: c_uchar)>;

#[cfg(all(feature = "native", target_os = "linux"))]
unsafe extern "C" {
    pub fn MmsInitialize(options: *const MmsOptions, service: ServiceCallback) -> CResult;
    pub fn MmsExit();
    pub fn MmsGet(item_list: *mut GetItems, item_num: c_uint) -> CResult;
    pub fn MmsDelete(item_list: *mut DeleteItems, item_num: c_uint) -> CResult;
    pub fn MmsReplace(item_list: *mut ReplaceItems, item_num: c_uint) -> CResult;
}
