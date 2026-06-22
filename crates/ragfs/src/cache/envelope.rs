//! Stable values stored behind the provider contract.

use super::{CacheError, CacheResult};
use crate::core::FileInfo;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const CACHE_ENVELOPE_MAGIC: &[u8; 4] = b"RGFC";
const CACHE_ENVELOPE_VERSION: u8 = 2;
const KIND_FILE: u8 = 1;
const KIND_DIRECTORY: u8 = 2;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum CacheObjectKind {
    File,
    Directory,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheEnvelope {
    version: u8,
    kind: CacheObjectKind,
    path: String,
    generations: Vec<GenerationSnapshot>,
    payload: CachePayload,
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

    pub fn encode(&self) -> CacheResult<Bytes> {
        let mut encoded = Vec::new();
        encoded.extend_from_slice(CACHE_ENVELOPE_MAGIC);
        encoded.push(CACHE_ENVELOPE_VERSION);
        encoded.push(match self.kind {
            CacheObjectKind::File => KIND_FILE,
            CacheObjectKind::Directory => KIND_DIRECTORY,
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
        };
        Ok(Self {
            version,
            kind,
            path,
            generations,
            payload,
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
            CachePayload::Directory(_) => {
                Err(CacheError::InvalidData("expected file payload".to_string()))
            }
        }
    }

    pub fn into_directory(self) -> CacheResult<Vec<FileInfo>> {
        match self.payload {
            CachePayload::Directory(entries) => Ok(entries),
            CachePayload::File(_) => Err(CacheError::InvalidData(
                "expected directory payload".to_string(),
            )),
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
        write_string(&mut encoded, &entry.name)?;
        encoded.extend_from_slice(&entry.size.to_be_bytes());
        encoded.extend_from_slice(&entry.mode.to_be_bytes());
        let (secs, nanos) = system_time_to_parts(entry.mod_time);
        encoded.extend_from_slice(&secs.to_be_bytes());
        encoded.extend_from_slice(&nanos.to_be_bytes());
        encoded.push(u8::from(entry.is_dir));
    }
    Ok(encoded)
}

fn decode_directory_payload(value: &[u8]) -> CacheResult<Vec<FileInfo>> {
    let mut reader = BinaryReader::new(value);
    let entry_count = reader.read_u32()? as usize;
    let mut entries = Vec::with_capacity(entry_count);
    for _ in 0..entry_count {
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
        entries.push(FileInfo {
            name,
            size,
            mode,
            mod_time: parts_to_system_time(secs, nanos),
            is_dir,
        });
    }
    if !reader.is_finished() {
        return Err(CacheError::InvalidData(
            "trailing bytes in directory payload".to_string(),
        ));
    }
    Ok(entries)
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
}
