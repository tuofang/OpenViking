#![cfg_attr(not(feature = "memstore-native"), allow(dead_code))]

use crate::MemStoreStoreError;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::ops::Range;

pub(crate) const HEADER_LEN: usize = 9;
const MAGIC: &[u8; 4] = b"OVMS";
const VERSION: u8 = 1;

pub(crate) fn validate_key(key: &str) -> Result<(), MemStoreStoreError> {
    if key.is_empty() {
        Err(MemStoreStoreError::InvalidArgument(
            "MemStore cache key must not be empty".into(),
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn native_key(key: &str) -> Result<String, MemStoreStoreError> {
    validate_key(key)?;
    if key.len() <= 127 {
        return Ok(key.to_owned());
    }

    let digest = Sha256::digest(key.as_bytes());
    let mut encoded = String::with_capacity(71);
    encoded.push_str("ovms:h:");
    for byte in digest {
        use std::fmt::Write;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    Ok(encoded)
}

pub(crate) fn encode_value(
    payload: &[u8],
    max_payload_size: usize,
) -> Result<Vec<u8>, MemStoreStoreError> {
    if payload.len() > max_payload_size {
        return Err(MemStoreStoreError::InvalidArgument(format!(
            "MemStore payload is {} bytes, exceeding the {} byte limit",
            payload.len(),
            max_payload_size
        )));
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        MemStoreStoreError::InvalidArgument("MemStore payload does not fit in u32".into())
    })?;
    let mut framed = Vec::with_capacity(HEADER_LEN + payload.len());
    framed.extend_from_slice(MAGIC);
    framed.push(VERSION);
    framed.extend_from_slice(&payload_len.to_be_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

#[cfg(test)]
pub(crate) fn decode_value(
    framed: &[u8],
    max_payload_size: usize,
) -> Result<Vec<u8>, MemStoreStoreError> {
    Ok(framed[payload_range(framed, max_payload_size)?].to_vec())
}

pub(crate) fn decode_owned_value(
    framed: Bytes,
    max_payload_size: usize,
) -> Result<Bytes, MemStoreStoreError> {
    let range = payload_range(&framed, max_payload_size)?;
    Ok(framed.slice(range))
}

pub(crate) fn payload_len(
    framed: &[u8],
    max_payload_size: usize,
) -> Result<usize, MemStoreStoreError> {
    if framed.len() < HEADER_LEN {
        return invalid_data("MemStore value frame is truncated");
    }
    if &framed[..4] != MAGIC {
        return invalid_data("MemStore value frame has invalid magic");
    }
    if framed[4] != VERSION {
        return invalid_data(format!(
            "MemStore value frame version {} is unsupported",
            framed[4]
        ));
    }
    let payload_len =
        u32::from_be_bytes(framed[5..9].try_into().expect("fixed frame header")) as usize;
    if payload_len > max_payload_size {
        return invalid_data(format!(
            "MemStore framed payload is {payload_len} bytes, exceeding the {max_payload_size} byte limit"
        ));
    }
    Ok(payload_len)
}

fn payload_range(
    framed: &[u8],
    max_payload_size: usize,
) -> Result<Range<usize>, MemStoreStoreError> {
    let payload_len = payload_len(framed, max_payload_size)?;
    let logical_len = HEADER_LEN.checked_add(payload_len).ok_or_else(|| {
        MemStoreStoreError::InvalidData("MemStore value frame length overflowed".into())
    })?;
    if framed.len() < logical_len {
        return invalid_data("MemStore value frame payload is truncated");
    }
    Ok(HEADER_LEN..logical_len)
}

fn invalid_data<T>(message: impl Into<String>) -> Result<T, MemStoreStoreError> {
    Err(MemStoreStoreError::InvalidData(message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemStoreStoreError;
    use bytes::Bytes;

    #[test]
    fn direct_and_hashed_keys_match_the_memstore_mapping() {
        let direct = "a".repeat(127);
        assert_eq!(native_key(&direct).unwrap(), direct);

        let long = "a".repeat(128);
        assert_eq!(
            native_key(&long).unwrap(),
            "ovms:h:6836cf13bac400e9105071cd6af47084dfacad4e5e302c94bfed24e013afb73e"
        );
        assert!(matches!(
            native_key(""),
            Err(MemStoreStoreError::InvalidArgument(_))
        ));
    }

    #[test]
    fn frame_format_and_empty_payload_are_exact() {
        assert_eq!(
            encode_value(b"abc", 16).unwrap(),
            b"OVMS\x01\x00\x00\x00\x03abc"
        );
        let empty = encode_value(b"", 16).unwrap();
        assert_eq!(empty, b"OVMS\x01\x00\x00\x00\x00");
        assert_eq!(decode_value(&empty, 16).unwrap(), b"");
    }

    #[test]
    fn decode_rejects_invalid_or_truncated_frames() {
        for frame in [
            b"BAD!\x01\x00\x00\x00\x00".as_slice(),
            b"OVMS\x02\x00\x00\x00\x00".as_slice(),
            b"OVMS\x01\x00\x00\x00".as_slice(),
            b"OVMS\x01\x00\x00\x00\x04abc".as_slice(),
        ] {
            assert!(matches!(
                decode_value(frame, 16),
                Err(MemStoreStoreError::InvalidData(_))
            ));
        }
    }

    #[test]
    fn decode_uses_logical_length_and_ignores_trailing_bytes() {
        assert_eq!(
            decode_value(b"OVMS\x01\x00\x00\x00\x03newstale", 16).unwrap(),
            b"new"
        );
    }

    #[test]
    fn owned_decode_slices_payload_without_copying() {
        let framed = Bytes::from(encode_value(b"payload", 16).unwrap());
        let payload_ptr = unsafe { framed.as_ptr().add(HEADER_LEN) };

        let decoded = decode_owned_value(framed, 16).unwrap();

        assert_eq!(decoded, Bytes::from_static(b"payload"));
        assert_eq!(decoded.as_ptr(), payload_ptr);
    }

    #[test]
    fn payload_size_limit_is_enforced_for_encode_and_decode() {
        assert!(matches!(
            encode_value(b"1234", 3),
            Err(MemStoreStoreError::InvalidArgument(_))
        ));
        assert!(matches!(
            decode_value(b"OVMS\x01\x00\x00\x00\x04data", 3),
            Err(MemStoreStoreError::InvalidData(_))
        ));
    }
}
