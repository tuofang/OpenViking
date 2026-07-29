use bytes::Bytes;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

pub(crate) struct ReadBufferPool {
    state: Mutex<ReadBufferPoolState>,
    max_buffers: usize,
    max_total_capacity: usize,
    max_buffer_capacity: usize,
}

#[derive(Default)]
struct ReadBufferPoolState {
    buffers: Vec<Vec<u8>>,
    total_capacity: usize,
}

pub(crate) struct ReadBufferLease {
    buffer: Option<Vec<u8>>,
    logical_len: usize,
    pool: Arc<ReadBufferPool>,
}

struct ReadBufferOwner {
    buffer: Option<Vec<u8>>,
    logical_len: usize,
    pool: Arc<ReadBufferPool>,
}

impl ReadBufferPool {
    pub(crate) fn new(
        max_buffers: usize,
        max_total_capacity: usize,
        max_buffer_capacity: usize,
    ) -> Self {
        Self {
            state: Mutex::new(ReadBufferPoolState::default()),
            max_buffers,
            max_total_capacity,
            max_buffer_capacity,
        }
    }

    pub(crate) fn take(self: &Arc<Self>, size: usize) -> ReadBufferLease {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let candidate = state
            .buffers
            .iter()
            .enumerate()
            .filter(|(_, buffer)| buffer.capacity() >= size)
            .min_by_key(|(_, buffer)| buffer.capacity())
            .map(|(index, _)| index);
        let mut buffer = match candidate {
            Some(index) => {
                let buffer = state.buffers.swap_remove(index);
                state.total_capacity = state.total_capacity.saturating_sub(buffer.capacity());
                buffer
            }
            None => Vec::with_capacity(size),
        };
        drop(state);
        if buffer.len() < size {
            buffer.resize(size, 0);
        }
        ReadBufferLease {
            buffer: Some(buffer),
            logical_len: size,
            pool: Arc::clone(self),
        }
    }

    fn recycle(&self, buffer: Vec<u8>) {
        let capacity = buffer.capacity();
        if capacity > self.max_buffer_capacity {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.buffers.len() >= self.max_buffers
            || state.total_capacity.saturating_add(capacity) > self.max_total_capacity
        {
            return;
        }
        state.total_capacity += capacity;
        state.buffers.push(buffer);
    }

    #[cfg(test)]
    fn cached_buffer_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .buffers
            .len()
    }
}

impl ReadBufferLease {
    pub(crate) fn into_bytes(mut self) -> Bytes {
        let owner = ReadBufferOwner {
            buffer: self.buffer.take(),
            logical_len: self.logical_len,
            pool: Arc::clone(&self.pool),
        };
        Bytes::from_owner(owner)
    }
}

impl Deref for ReadBufferLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.buffer.as_deref().expect("read buffer lease is empty")[..self.logical_len]
    }
}

impl DerefMut for ReadBufferLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .buffer
            .as_deref_mut()
            .expect("read buffer lease is empty")[..self.logical_len]
    }
}

impl Drop for ReadBufferLease {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.recycle(buffer);
        }
    }
}

impl AsRef<[u8]> for ReadBufferOwner {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.as_deref().expect("read buffer owner is empty")[..self.logical_len]
    }
}

impl Drop for ReadBufferOwner {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.recycle(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn pooled_bytes_return_the_same_allocation_after_last_owner_drops() {
        let pool = Arc::new(ReadBufferPool::new(2, 1_024, 1_024));
        let lease = pool.take(128);
        let allocation = lease.as_ptr();

        let bytes = lease.into_bytes();
        assert_eq!(bytes.as_ptr(), allocation);
        assert_eq!(pool.cached_buffer_count(), 0);
        drop(bytes);

        assert_eq!(pool.cached_buffer_count(), 1);
        let reused = pool.take(128);
        assert_eq!(reused.as_ptr(), allocation);
    }

    #[test]
    fn oversized_buffers_are_not_retained() {
        let pool = Arc::new(ReadBufferPool::new(2, 1_024, 64));

        drop(pool.take(128));

        assert_eq!(pool.cached_buffer_count(), 0);
    }

    #[test]
    fn reused_buffers_keep_initialized_bytes_without_rezeroing() {
        let pool = Arc::new(ReadBufferPool::new(1, 1_024, 1_024));
        let mut lease = pool.take(128);
        lease.fill(0x5a);
        drop(lease);

        let reused = pool.take(64);

        assert!(reused.iter().all(|byte| *byte == 0x5a));
    }
}
