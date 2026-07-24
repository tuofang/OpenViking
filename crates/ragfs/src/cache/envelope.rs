//! Stable values stored behind the provider contract.

use super::{CacheError, CacheResult};
use crate::core::FileInfo;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CACHE_ENVELOPE_MAGIC: &[u8; 4] = b"RGFC";
const CACHE_ENVELOPE_VERSION: u8 = 2;
const KIND_FILE: u8 = 1;
const KIND_DIRECTORY: u8 = 2;
const KIND_STAT: u8 = 3;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum CacheObjectKind {
    File,
    Directory,
    Stat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GenerationSnapshot {
    pub key: String,
    pub value: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum CachePayload {
    File(Vec<u8>),
    Directory(Vec<FileInfo>),
    Stat(FileInfo),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheEnvelope {
    version: u8,
    kind: CacheObjectKind,
    path: String,
    generations: Vec<GenerationSnapshot>,
    payload: CachePayload,
}

#[cfg(test)]
pub(crate) struct FileEnvelopeView<'a> {
    path: String,
    generations: Vec<GenerationSnapshot>,
    payload: &'a [u8],
}

pub(crate) struct FileEnvelopeParts {
    path: String,
    generations: Vec<GenerationSnapshot>,
    payload_range: Range<usize>,
}

#[cfg(test)]
impl<'a> FileEnvelopeView<'a> {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn generations(&self) -> &[GenerationSnapshot] {
        &self.generations
    }

    pub fn payload(&self) -> &'a [u8] {
        self.payload
    }
}

impl FileEnvelopeParts {
    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn into_generations(self) -> Vec<GenerationSnapshot> {
        self.generations
    }

    pub fn payload_range(&self) -> Range<usize> {
        self.payload_range.clone()
    }
}

impl CacheEnvelope {
    pub fn file(path: String, data: Vec<u8>, generations: Vec<GenerationSnapshot>) -> Self {
        Self {
            version: CACHE_ENVELOPE_VERSION,
            kind: CacheObjectKind::File,
            path,
            generations,
            payload: CachePayload::File(data),
        }
    }

    pub fn directory(
        path: String,
        entries: Vec<FileInfo>,
        generations: Vec<GenerationSnapshot>,
    ) -> Self {
        Self {
            version: CACHE_ENVELOPE_VERSION,
            kind: CacheObjectKind::Directory,
            path,
            generations,
            payload: CachePayload::Directory(entries),
        }
    }

    pub fn stat(path: String, info: FileInfo, generations: Vec<GenerationSnapshot>) -> Self {
        Self {
            version: CACHE_ENVELOPE_VERSION,
            kind: CacheObjectKind::Stat,
            path,
            generations,
            payload: CachePayload::Stat(info),
        }
    }

    pub fn encode(&self) -> CacheResult<Bytes> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(CACHE_ENVELOPE_MAGIC);
        encoded.push(CACHE_ENVELOPE_VERSION);
        encoded.push(match self.kind {
            CacheObjectKind::File => KIND_FILE,
            CacheObjectKind::Directory => KIND_DIRECTORY,
            CacheObjectKind::Stat => KIND_STAT,
        });
        write_string(&mut encoded, &self.path)?;
        write_u32(&mut encoded, self.generations.len())?;
        for snapshot in &self.generations {
            write_string(&mut encoded, &snapshot.key)?;
            encoded.extend_from_slice(&snapshot.value.to_be_bytes());
        }
        match &self.payload {
            CachePayload::File(data) => {
                write_u64(&mut encoded, data.len())?;
                encoded.extend_from_slice(data);
            }
            CachePayload::Directory(entries) => {
                let payload = encode_directory_payload(entries)?;
                write_u64(&mut encoded, payload.len())?;
                encoded.extend_from_slice(&payload);
            }
            CachePayload::Stat(info) => {
                let payload = encode_file_info_payload(info)?;
                write_u64(&mut encoded, payload.len())?;
                encoded.extend_from_slice(&payload);
            }
        }
        Ok(Bytes::from(encoded))
    }

    pub fn decode(value: &[u8]) -> CacheResult<Self> {
        let mut reader = BinaryReader::new(value);
        let magic = reader.read_bytes(CACHE_ENVELOPE_MAGIC.len())?;
        if magic != CACHE_ENVELOPE_MAGIC {
            return Err(CacheError::InvalidData(
                "invalid envelope magic".to_string(),
            ));
        }
        let version = reader.read_u8()?;
        if version != CACHE_ENVELOPE_VERSION {
            return Err(CacheError::InvalidData(format!(
                "unsupported envelope version {version}"
            )));
        }
        let kind = match reader.read_u8()? {
            KIND_FILE => CacheObjectKind::File,
            KIND_DIRECTORY => CacheObjectKind::Directory,
            KIND_STAT => CacheObjectKind::Stat,
            other => {
                return Err(CacheError::InvalidData(format!(
                    "unsupported envelope kind {other}"
                )))
            }
        };
        let path = reader.read_string()?;
        let generation_count = reader.read_u32()? as usize;
        let mut generations = Vec::with_capacity(generation_count);
        for _ in 0..generation_count {
            generations.push(GenerationSnapshot {
                key: reader.read_string()?,
                value: reader.read_u64()?,
            });
        }
        let payload_len = reader.read_u64()? as usize;
        let payload_bytes = reader.read_bytes(payload_len)?;
        if !reader.is_finished() {
            return Err(CacheError::InvalidData(
                "trailing bytes in cache envelope".to_string(),
            ));
        }
        let payload = match kind {
            CacheObjectKind::File => CachePayload::File(payload_bytes.to_vec()),
            CacheObjectKind::Directory => {
                CachePayload::Directory(decode_directory_payload(payload_bytes)?)
            }
            CacheObjectKind::Stat => CachePayload::Stat(decode_file_info_payload(payload_bytes)?),
        };
        Ok(Self {
            version,
            kind,
            path,
            generations,
            payload,
        })
    }

    #[cfg(test)]
    pub fn decode_file_view(value: &[u8]) -> CacheResult<FileEnvelopeView<'_>> {
        let parts = Self::decode_file_parts(value)?;
        Ok(FileEnvelopeView {
            path: parts.path,
            generations: parts.generations,
            payload: &value[parts.payload_range],
        })
    }

    pub fn decode_file_parts(value: &[u8]) -> CacheResult<FileEnvelopeParts> {
        let mut reader = BinaryReader::new(value);
        let magic = reader.read_bytes(CACHE_ENVELOPE_MAGIC.len())?;
        if magic != CACHE_ENVELOPE_MAGIC {
            return Err(CacheError::InvalidData(
                "invalid envelope magic".to_string(),
            ));
        }
        let version = reader.read_u8()?;
        if version != CACHE_ENVELOPE_VERSION {
            return Err(CacheError::InvalidData(format!(
                "unsupported envelope version {version}"
            )));
        }
        let kind = match reader.read_u8()? {
            KIND_FILE => CacheObjectKind::File,
            KIND_DIRECTORY => CacheObjectKind::Directory,
            KIND_STAT => CacheObjectKind::Stat,
            other => {
                return Err(CacheError::InvalidData(format!(
                    "unsupported envelope kind {other}"
                )))
            }
        };
        if kind != CacheObjectKind::File {
            return Err(CacheError::InvalidData(
                "expected file envelope".to_string(),
            ));
        }
        let path = reader.read_string()?;
        let generation_count = reader.read_u32()? as usize;
        let mut generations = Vec::with_capacity(generation_count);
        for _ in 0..generation_count {
            generations.push(GenerationSnapshot {
                key: reader.read_string()?,
                value: reader.read_u64()?,
            });
        }
        let payload_len = reader.read_u64()? as usize;
        let payload_start = reader.position();
        reader.read_bytes(payload_len)?;
        let payload_end = reader.position();
        if !reader.is_finished() {
            return Err(CacheError::InvalidData(
                "trailing bytes in cache envelope".to_string(),
            ));
        }
        Ok(FileEnvelopeParts {
            path,
            generations,
            payload_range: payload_start..payload_end,
        })
    }

    pub fn matches(&self, kind: CacheObjectKind, path: &str) -> bool {
        self.kind == kind && self.path == path
    }

    pub fn generations(&self) -> &[GenerationSnapshot] {
        &self.generations
    }

    pub fn into_file(self) -> CacheResult<Vec<u8>> {
        match self.payload {
            CachePayload::File(data) => Ok(data),
            CachePayload::Directory(_) | CachePayload::Stat(_) => {
                Err(CacheError::InvalidData("expected file payload".to_string()))
            }
        }
    }

    pub fn into_directory(self) -> CacheResult<Vec<FileInfo>> {
        match self.payload {
            CachePayload::Directory(entries) => Ok(entries),
            CachePayload::File(_) | CachePayload::Stat(_) => Err(CacheError::InvalidData(
                "expected directory payload".to_string(),
            )),
        }
    }

    pub fn into_stat(self) -> CacheResult<FileInfo> {
        match self.payload {
            CachePayload::Stat(info) => Ok(info),
            CachePayload::File(_) | CachePayload::Directory(_) => {
                Err(CacheError::InvalidData("expected stat payload".to_string()))
            }
        }
    }
}

fn write_u32(buffer: &mut Vec<u8>, value: usize) -> CacheResult<()> {
    let value = u32::try_from(value)
        .map_err(|_| CacheError::InvalidData("cache envelope field too large".to_string()))?;
    buffer.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn write_u64(buffer: &mut Vec<u8>, value: usize) -> CacheResult<()> {
    let value = u64::try_from(value)
        .map_err(|_| CacheError::InvalidData("cache envelope payload too large".to_string()))?;
    buffer.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn write_string(buffer: &mut Vec<u8>, value: &str) -> CacheResult<()> {
    write_u32(buffer, value.len())?;
    buffer.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_directory_payload(entries: &[FileInfo]) -> CacheResult<Vec<u8>> {
    let mut encoded = Vec::new();
    write_u32(&mut encoded, entries.len())?;
    for entry in entries {
        encode_file_info(&mut encoded, entry)?;
    }
    Ok(encoded)
}

fn encode_file_info_payload(info: &FileInfo) -> CacheResult<Vec<u8>> {
    let mut encoded = Vec::new();
    encode_file_info(&mut encoded, info)?;
    Ok(encoded)
}

fn encode_file_info(encoded: &mut Vec<u8>, info: &FileInfo) -> CacheResult<()> {
    write_string(encoded, &info.name)?;
    encoded.extend_from_slice(&info.size.to_be_bytes());
    encoded.extend_from_slice(&info.mode.to_be_bytes());
    let (secs, nanos) = system_time_to_parts(info.mod_time);
    encoded.extend_from_slice(&secs.to_be_bytes());
    encoded.extend_from_slice(&nanos.to_be_bytes());
    encoded.push(u8::from(info.is_dir));
    Ok(())
}

fn decode_directory_payload(value: &[u8]) -> CacheResult<Vec<FileInfo>> {
    let mut reader = BinaryReader::new(value);
    let entry_count = reader.read_u32()? as usize;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
        entries.push(decode_file_info(&mut reader)?);
    }
    if !reader.is_finished() {
        return Err(CacheError::InvalidData(
            "trailing bytes in directory payload".to_string(),
        ));
    }
    Ok(entries)
}

fn decode_file_info_payload(value: &[u8]) -> CacheResult<FileInfo> {
    let mut reader = BinaryReader::new(value);
    let info = decode_file_info(&mut reader)?;
    if !reader.is_finished() {
        return Err(CacheError::InvalidData(
            "trailing bytes in stat payload".to_string(),
        ));
    }
    Ok(info)
}

fn decode_file_info(reader: &mut BinaryReader<'_>) -> CacheResult<FileInfo> {
    let name = reader.read_string()?;
    let size = reader.read_u64()?;
    let mode = reader.read_u32()?;
    let secs = reader.read_i64()?;
    let nanos = reader.read_u32()?;
    if nanos >= 1_000_000_000 {
        return Err(CacheError::InvalidData(
            "invalid FileInfo timestamp nanos".to_string(),
        ));
    }
    let is_dir = match reader.read_u8()? {
        0 => false,
        1 => true,
        _ => {
            return Err(CacheError::InvalidData(
                "invalid FileInfo directory flag".to_string(),
            ))
        }
    };
    Ok(FileInfo {
        name,
        size,
        mode,
        mod_time: parts_to_system_time(secs, nanos),
        is_dir,
    })
}

fn system_time_to_parts(value: SystemTime) -> (i64, u32) {
    match value.duration_since(UNIX_EPOCH) {
        Ok(duration) => (
            duration.as_secs().min(i64::MAX as u64) as i64,
            duration.subsec_nanos(),
        ),
        Err(error) => {
            let duration = error.duration();
            (
                -(duration.as_secs().min(i64::MAX as u64) as i64),
                duration.subsec_nanos(),
            )
        }
    }
}

fn parts_to_system_time(secs: i64, nanos: u32) -> SystemTime {
    let duration = Duration::new(secs.unsigned_abs(), nanos);
    if secs >= 0 {
        UNIX_EPOCH + duration
    } else {
        UNIX_EPOCH - duration
    }
}

struct BinaryReader<'a> {
    value: &'a [u8],
    position: usize,
}

impl<'a> BinaryReader<'a> {
    fn new(value: &'a [u8]) -> Self {
        Self { value, position: 0 }
    }

    fn is_finished(&self) -> bool {
        self.position == self.value.len()
    }

    fn position(&self) -> usize {
        self.position
    }

    fn read_bytes(&mut self, len: usize) -> CacheResult<&'a [u8]> {
        let end = self
            .position
            .checked_add(len)
            .ok_or_else(|| CacheError::InvalidData("cache envelope offset overflow".to_string()))?;
        if end > self.value.len() {
            return Err(CacheError::InvalidData(
                "truncated cache envelope".to_string(),
            ));
        }
        let bytes = &self.value[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> CacheResult<u8> {
        Ok(self.read_bytes(1)?[0])
    }

    fn read_u32(&mut self) -> CacheResult<u32> {
        let bytes: [u8; 4] = self
            .read_bytes(4)?
            .try_into()
            .map_err(|_| CacheError::InvalidData("invalid u32 in cache envelope".to_string()))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> CacheResult<u64> {
        let bytes: [u8; 8] = self
            .read_bytes(8)?
            .try_into()
            .map_err(|_| CacheError::InvalidData("invalid u64 in cache envelope".to_string()))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_i64(&mut self) -> CacheResult<i64> {
        let bytes: [u8; 8] = self
            .read_bytes(8)?
            .try_into()
            .map_err(|_| CacheError::InvalidData("invalid i64 in cache envelope".to_string()))?;
        Ok(i64::from_be_bytes(bytes))
    }

    fn read_string(&mut self) -> CacheResult<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|error| CacheError::InvalidData(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_envelope_encodes_as_binary_v2_with_raw_payload() {
        let data = b"deployment deployment deployment".to_vec();
        let envelope = CacheEnvelope::file(
            "/docs/a.txt".to_string(),
            data.clone(),
            vec![GenerationSnapshot {
                key: "ragfs:v2:test:subtree:0000000000000001".to_string(),
                value: 42,
            }],
        );

        let encoded = envelope.encode().unwrap();

        assert_eq!(&encoded[..5], b"RGFC\x02");
        assert!(
            encoded.windows(data.len()).any(|window| window == data),
            "binary file envelope should store file bytes without JSON number-array expansion"
        );
        assert_eq!(
            CacheEnvelope::decode(&encoded)
                .unwrap()
                .into_file()
                .unwrap(),
            data
        );
    }

    #[test]
    fn file_envelope_view_borrows_payload_without_copying() {
        let data = b"borrowed file payload".to_vec();
        let envelope = CacheEnvelope::file(
            "/docs/a.txt".to_string(),
            data.clone(),
            vec![GenerationSnapshot {
                key: "ragfs:v2:test:subtree:0000000000000001".to_string(),
                value: 42,
            }],
        );

        let encoded = envelope.encode().unwrap();
        let view = CacheEnvelope::decode_file_view(&encoded).unwrap();

        assert_eq!(view.path(), "/docs/a.txt");
        assert_eq!(view.generations()[0].value, 42);
        assert_eq!(view.payload(), data.as_slice());

        let encoded_start = encoded.as_ptr() as usize;
        let encoded_end = encoded_start + encoded.len();
        let payload_ptr = view.payload().as_ptr() as usize;
        assert!(
            (encoded_start..encoded_end).contains(&payload_ptr),
            "file view payload should borrow from the encoded envelope buffer"
        );
    }

    #[test]
    fn stat_envelope_round_trips_file_info() {
        let info = FileInfo::new(
            "a.txt".to_string(),
            123,
            0o640,
            UNIX_EPOCH + Duration::new(42, 123_456_789),
            false,
        );
        let envelope = CacheEnvelope::stat(
            "/docs/a.txt".to_string(),
            info,
            vec![GenerationSnapshot {
                key: "ragfs:v2:test:subtree:0000000000000001".to_string(),
                value: 7,
            }],
        );

        let decoded = CacheEnvelope::decode(&envelope.encode().unwrap()).unwrap();
        assert!(decoded.matches(CacheObjectKind::Stat, "/docs/a.txt"));
        let decoded = decoded.into_stat().unwrap();
        assert_eq!(decoded.name, "a.txt");
        assert_eq!(decoded.size, 123);
        assert_eq!(decoded.mode, 0o640);
        assert_eq!(
            decoded.mod_time,
            UNIX_EPOCH + Duration::new(42, 123_456_789)
        );
        assert!(!decoded.is_dir);
    }
}
