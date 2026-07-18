# MemStore Preallocated Read Design

## Goal

Remove the unconditional 9-byte header `MmsGet` from normal MemStore cache reads. Values whose framed representation fits in a 4 KiB caller-owned buffer must complete in one `MmsGet`; only oversized or concurrently growing values may require another call.

## Scope

- Change only the native MemStore read path and its contract tests.
- Keep the existing `OVMS | version | payload_len | payload` frame format.
- Keep the existing 64 MiB maximum value limit and resize-attempt limit.
- Do not add public configuration or change `CacheProvider` behavior.
- Keep existence probes and health checks on the 9-byte header read because they do not need the payload.

## Read Algorithm

For each key in `get` or `batch_get`, calculate an initial frame buffer size:

```text
min(4096, HEADER_LEN + max_value_size_bytes)
```

Issue one batch `MmsGet` with that size for every key.

- Misses and per-item errors finish immediately.
- Successful reads validate the frame header in the initial buffer.
- If the logical frame length is no larger than the supplied buffer and `realLength` covers the logical frame, decode immediately. Trailing bytes remain ignored so short-over-long replacement stays correct.
- If the logical frame length exceeds the supplied buffer, retry only that key with the exact required frame length.
- If a value grows between calls, use the new header length and retry that key again, up to the existing three-resize limit.
- A replaced caller buffer remains an internal error.

The initial result may be either `RET_MMS_OK` or `RET_MMS_READ_EXCEED`. `RET_MMS_READ_EXCEED` does not force a retry when the logical frame already fits in the caller buffer; this is required for short-over-long replacement, where MemStore can report a stale physical tail through `realLength`.

## Tests

The native contract fake records each `MmsGet` call and requested buffer length. Tests must prove:

- A 1 KiB payload is returned by one `MmsGet` using a 4 KiB initial buffer.
- Empty and short-over-long values are decoded directly from the initial read.
- A value larger than 4 KiB causes one exact-size retry.
- A mixed batch retries only oversized keys.
- A value that grows between reads continues through the bounded resize loop.
- Existing miss, invalid frame, zero-copy replacement, sentinel status, and resize-exhaustion behavior remains unchanged.

## Expected Effect

The 1 KiB hot path removes the header `MmsGet`, previously about 32 microseconds in the measured environment. The remaining cost is one full MemStore read plus the existing blocking-task bridge and frame decode.
