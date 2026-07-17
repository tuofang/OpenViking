use std::ffi::{c_char, c_uint, c_ushort};
use std::mem::{align_of, offset_of, size_of};

use ragfs_cache_memstore_sys::{
    CResult, DeleteItems, GetItems, MmsOptions, PutItems, ReplaceItems, UpdateItems, RET_MMS_BUSY,
    RET_MMS_CONFLICT, RET_MMS_EPERM, RET_MMS_ERROR, RET_MMS_EXCEED_QUOTA, RET_MMS_EXISTS,
    RET_MMS_MISS, RET_MMS_NEED_RETRY, RET_MMS_NOT_FOUND, RET_MMS_NOT_READY, RET_MMS_NO_SPACE,
    RET_MMS_OK, RET_MMS_PROTECTED, RET_MMS_PT_FAULT, RET_MMS_READ_EXCEED, RET_MMS_UNAVAILABLE,
};

fn aligned_offset(offset: usize, alignment: usize) -> usize {
    (offset + alignment - 1) & !(alignment - 1)
}

fn c_layout(field_types: &[(usize, usize)]) -> (Vec<usize>, usize, usize) {
    let mut offsets = Vec::with_capacity(field_types.len());
    let mut offset = 0;
    let mut alignment = 1;

    for &(size, field_alignment) in field_types {
        offset = aligned_offset(offset, field_alignment);
        offsets.push(offset);
        offset += size;
        alignment = alignment.max(field_alignment);
    }

    (offsets, aligned_offset(offset, alignment), alignment)
}

#[test]
fn c_layout_math_catches_the_32_bit_pointer_padding_case() {
    let (offsets, size, alignment) =
        c_layout(&[(4, 4), (4, 4), (4, 4), (2, 2), (2, 2), (4, 4), (4, 4)]);

    assert_eq!(offsets, [0, 4, 8, 12, 14, 16, 20]);
    assert_eq!(size, 24);
    assert_eq!(alignment, 4);
    assert_ne!(size, 4 * 5);
}

#[test]
fn c_result_values_match_mms_c_header() {
    let values: [CResult; 16] = [
        RET_MMS_OK,
        RET_MMS_PROTECTED,
        RET_MMS_ERROR,
        RET_MMS_EPERM,
        RET_MMS_BUSY,
        RET_MMS_NEED_RETRY,
        RET_MMS_NOT_READY,
        RET_MMS_NOT_FOUND,
        RET_MMS_CONFLICT,
        RET_MMS_MISS,
        RET_MMS_NO_SPACE,
        RET_MMS_UNAVAILABLE,
        RET_MMS_EXCEED_QUOTA,
        RET_MMS_PT_FAULT,
        RET_MMS_READ_EXCEED,
        RET_MMS_EXISTS,
    ];

    assert_eq!(values, core::array::from_fn(|index| index as CResult));
}

#[test]
fn mms_options_matches_c_layout() {
    let path = [size_of::<[c_char; libc::PATH_MAX as usize]>(); 7];
    let (offsets, size, alignment) = c_layout(&[
        (size_of::<c_ushort>(), align_of::<c_ushort>()),
        (size_of::<c_ushort>(), align_of::<c_ushort>()),
        (size_of::<libc::c_uchar>(), align_of::<libc::c_uchar>()),
        (size_of::<libc::c_uchar>(), align_of::<libc::c_uchar>()),
        (path[0], align_of::<[c_char; libc::PATH_MAX as usize]>()),
        (path[1], align_of::<[c_char; libc::PATH_MAX as usize]>()),
        (path[2], align_of::<[c_char; libc::PATH_MAX as usize]>()),
        (path[3], align_of::<[c_char; libc::PATH_MAX as usize]>()),
        (path[4], align_of::<[c_char; libc::PATH_MAX as usize]>()),
        (path[5], align_of::<[c_char; libc::PATH_MAX as usize]>()),
        (path[6], align_of::<[c_char; libc::PATH_MAX as usize]>()),
    ]);

    assert_eq!(align_of::<MmsOptions>(), alignment);
    assert_eq!(
        &[
            offset_of!(MmsOptions, net_connect_cnt),
            offset_of!(MmsOptions, net_group_num),
            offset_of!(MmsOptions, net_is_busy_polling),
            offset_of!(MmsOptions, tls_enable),
            offset_of!(MmsOptions, certification_path),
            offset_of!(MmsOptions, ca_cer_path),
            offset_of!(MmsOptions, ca_crl_path),
            offset_of!(MmsOptions, private_key_path),
            offset_of!(MmsOptions, private_key_password_path),
            offset_of!(MmsOptions, decrypter_lib_path),
            offset_of!(MmsOptions, openssl_lib_dir),
        ],
        offsets.as_slice()
    );
    assert_eq!(size_of::<MmsOptions>(), size);
}

#[test]
fn item_descriptors_match_c_layout() {
    let (put_offsets, put_size, put_alignment) = c_layout(&[
        (size_of::<*const c_char>(), align_of::<*const c_char>()),
        (size_of::<*const c_char>(), align_of::<*const c_char>()),
        (size_of::<c_uint>(), align_of::<c_uint>()),
        (size_of::<c_ushort>(), align_of::<c_ushort>()),
        (size_of::<c_ushort>(), align_of::<c_ushort>()),
        (
            size_of::<*mut *mut c_char>(),
            align_of::<*mut *mut c_char>(),
        ),
        (size_of::<*mut i32>(), align_of::<*mut i32>()),
    ]);
    assert_eq!(align_of::<PutItems>(), put_alignment);
    assert_eq!(
        &[
            offset_of!(PutItems, key),
            offset_of!(PutItems, value),
            offset_of!(PutItems, value_len),
            offset_of!(PutItems, key_len),
            offset_of!(PutItems, is_notify),
            offset_of!(PutItems, value_addr),
            offset_of!(PutItems, result),
        ],
        put_offsets.as_slice()
    );
    assert_eq!(size_of::<PutItems>(), put_size);

    let (get_offsets, get_size, get_alignment) = c_layout(&[
        (size_of::<*const c_char>(), align_of::<*const c_char>()),
        (size_of::<c_ushort>(), align_of::<c_ushort>()),
        (size_of::<c_uint>(), align_of::<c_uint>()),
        (size_of::<c_uint>(), align_of::<c_uint>()),
        (
            size_of::<*mut *mut c_char>(),
            align_of::<*mut *mut c_char>(),
        ),
        (size_of::<*mut c_uint>(), align_of::<*mut c_uint>()),
        (size_of::<*mut i32>(), align_of::<*mut i32>()),
    ]);
    assert_eq!(align_of::<GetItems>(), get_alignment);
    assert_eq!(
        &[
            offset_of!(GetItems, key),
            offset_of!(GetItems, key_len),
            offset_of!(GetItems, offset),
            offset_of!(GetItems, length),
            offset_of!(GetItems, value),
            offset_of!(GetItems, real_length),
            offset_of!(GetItems, result),
        ],
        get_offsets.as_slice()
    );
    assert_eq!(size_of::<GetItems>(), get_size);

    let (update_offsets, update_size, update_alignment) = c_layout(&[
        (size_of::<*const c_char>(), align_of::<*const c_char>()),
        (size_of::<*const c_char>(), align_of::<*const c_char>()),
        (size_of::<c_ushort>(), align_of::<c_ushort>()),
        (size_of::<c_uint>(), align_of::<c_uint>()),
        (size_of::<c_uint>(), align_of::<c_uint>()),
        (size_of::<*mut i32>(), align_of::<*mut i32>()),
    ]);
    assert_eq!(align_of::<UpdateItems>(), update_alignment);
    assert_eq!(
        &[
            offset_of!(UpdateItems, key),
            offset_of!(UpdateItems, value),
            offset_of!(UpdateItems, key_len),
            offset_of!(UpdateItems, value_len),
            offset_of!(UpdateItems, offset),
            offset_of!(UpdateItems, result),
        ],
        update_offsets.as_slice()
    );
    assert_eq!(size_of::<UpdateItems>(), update_size);
    assert_eq!(size_of::<ReplaceItems>(), size_of::<UpdateItems>());
    assert_eq!(align_of::<ReplaceItems>(), align_of::<UpdateItems>());

    let (delete_offsets, delete_size, delete_alignment) = c_layout(&[
        (size_of::<*const c_char>(), align_of::<*const c_char>()),
        (size_of::<c_ushort>(), align_of::<c_ushort>()),
        (size_of::<c_ushort>(), align_of::<c_ushort>()),
        (size_of::<*mut i32>(), align_of::<*mut i32>()),
    ]);
    assert_eq!(align_of::<DeleteItems>(), delete_alignment);
    assert_eq!(
        &[
            offset_of!(DeleteItems, key),
            offset_of!(DeleteItems, key_len),
            offset_of!(DeleteItems, is_notify),
            offset_of!(DeleteItems, result),
        ],
        delete_offsets.as_slice()
    );
    assert_eq!(size_of::<DeleteItems>(), delete_size);
}
