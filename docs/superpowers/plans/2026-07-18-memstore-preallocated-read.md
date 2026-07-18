# MemStore Preallocated Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete MemStore cache reads that fit in a 4 KiB framed buffer with one `MmsGet`, retrying only oversized or concurrently growing values.

**Architecture:** Replace the dedicated header-read phase with a unified bounded read loop. The first batch allocates 4 KiB per key (capped by the configured maximum frame size); frame headers determine whether each item can decode immediately or needs an exact-size retry. Existing frame semantics, stale-tail handling, and per-item error behavior remain unchanged.

**Tech Stack:** Rust 1.91.1, Tokio, MemStore C API, Cargo integration tests.

## Global Constraints

- Keep the OVMS v1 frame format unchanged.
- Do not add public configuration.
- Keep `MAX_RESIZE_ATTEMPTS=3` and `max_value_size_bytes` enforcement.
- Keep health and existence probes on header-only reads.
- Use TDD: contract tests must fail before production code changes.

---

### Task 1: Specify Single-Read and Selective-Retry Behavior

**Files:**
- Modify: `crates/ragfs-cache-memstore/tests/native_contract.rs`

**Interfaces:**
- Consumes: fake `MmsGet` implementation and `MemStoreProvider::get/batch_get`.
- Produces: recorded `MmsGet` call shapes as `Vec<Vec<(String, usize)>>` and assertions for 4 KiB initial reads.

- [ ] **Step 1: Record native GET calls in the fake store**

Add `get_calls: Vec<Vec<(String, usize)>>` to `FakeMemStore`. At the start of fake `MmsGet`, record every item's key and requested `length` before applying special-key behavior.

- [ ] **Step 2: Add small and oversized values to the contract test**

Raise the test-only maximum payload to 8 KiB. Store a 1 KiB value and a 5 KiB value, clear `get_calls`, then call `batch_get` with both keys.

- [ ] **Step 3: Assert the desired call sequence**

Require the first call to contain both keys with 4096-byte buffers and the second call to contain only the 5 KiB key with a `5009`-byte frame buffer. Also assert that a 1 KiB single-key GET records exactly one 4096-byte call.

- [ ] **Step 4: Update the growth simulation**

Make `GROWING_KEY` start with a framed payload larger than 4 KiB. After its first 4096-byte read, replace the fake stored value with a larger framed payload so the next exact-size read must resize once more.

- [ ] **Step 5: Run the native contract remotely and verify RED**

Run on node74 with the MemStore SDK:

```bash
cargo test --manifest-path crates/ragfs-cache-memstore/Cargo.toml \
  --features memstore-native --test native_contract -- --nocapture
```

Expected: FAIL because the current implementation requests 9-byte header buffers before full reads.

### Task 2: Implement the Unified Preallocated Read Loop

**Files:**
- Modify: `crates/ragfs-cache-memstore/src/native.rs`

**Interfaces:**
- Consumes: `raw_get`, `payload_len`, `decode_value`, `PendingRead`.
- Produces: `read_results(&self, keys)` with one-read completion for frames up to 4096 bytes.

- [ ] **Step 1: Define the private initial buffer constant**

Add:

```rust
const INITIAL_READ_BUFFER_SIZE: usize = 4 * 1024;
```

- [ ] **Step 2: Initialize all keys as pending reads**

Calculate the maximum possible frame size with checked addition and initialize each key with:

```rust
buffer_size: INITIAL_READ_BUFFER_SIZE.min(max_frame_size),
resize_attempts: 0,
```

- [ ] **Step 3: Replace header and full phases with one loop**

For every successful `RET_MMS_OK` or `RET_MMS_READ_EXCEED` item:

```rust
let payload_size = payload_len(&read.buffer, self.max_value_size)?;
let required_size = HEADER_LEN + payload_size;
if required_size > item.buffer_size {
    retry only this item with required_size;
} else if read.real_length < required_size {
    return InvalidData;
} else {
    decode_value(&read.buffer, self.max_value_size)
}
```

Preserve caller-buffer checks, miss handling, per-item status mapping, and the three-resize error.

- [ ] **Step 4: Run native contract and verify GREEN**

Run the Task 1 command. Expected: one test passes, zero failures.

- [ ] **Step 5: Run non-native MemStore tests**

```bash
cargo test --manifest-path crates/ragfs-cache-memstore/Cargo.toml
```

Expected: 26 tests pass, native-feature tests remain filtered out.

### Task 3: Format and Validate the Native SDK Path

**Files:**
- Verify: `crates/ragfs-cache-memstore/src/native.rs`
- Verify: `crates/ragfs-cache-memstore/tests/native_contract.rs`

**Interfaces:**
- Consumes: completed implementation and existing node74 MemStore SDK.
- Produces: formatted code and fresh test evidence.

- [ ] **Step 1: Format and inspect the diff**

```bash
cargo fmt --manifest-path crates/ragfs-cache-memstore/Cargo.toml -- --check
git diff --check
git diff -- crates/ragfs-cache-memstore/src/native.rs crates/ragfs-cache-memstore/tests/native_contract.rs
```

- [ ] **Step 2: Run the native round-trip smoke on node74**

```bash
OPENVIKING_RUN_MEMSTORE_INTEGRATION=true \
MEMSTORE_OPERATION_TIMEOUT_MS=15000 \
cargo test --manifest-path crates/ragfs-cache-memstore/Cargo.toml \
  --features memstore-native --test native_smoke -- --nocapture
```

Expected: replace/get, overwrite, batch, delete, and close all pass.

- [ ] **Step 3: Confirm only scoped files changed**

```bash
git status --short
```

Expected: only the two MemStore implementation/test files plus the approved design and plan documents differ from `fork/ragfs-add-cache`.
