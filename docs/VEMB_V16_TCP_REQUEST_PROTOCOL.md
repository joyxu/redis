# VEMB v16 TCP Request Protocol

## Goal

Define the TCP request payload format between `bench_cli` and `proxy`.

This protocol replaces the previous TCP request layout entirely.

- Keep the existing outer `vemb_v16_net_hdr_t` frame.
- Replace the request payload with a compact per-op encoding.
- Decode the compact TCP payload into internal `vemb_v16_req_t` after receive.
- Aeron/ring-buffer transport remains unchanged.

## Motivation

The old TCP request path reused a wide request struct layout, which caused
lightweight ops such as `VEMB_INLINE` and `VREM` to carry unused request
fields on the wire.

This protocol makes TCP request size proportional to the actual op.

## Non-Goals

- No change to response encoding in this document.
- No change to SHM/Aeron request layout.
- No conversion to RESP or argv-style internal execution.
- `VEMB_HANDLE` is not a TCP operation. TCP clients use `VEMB_INLINE`; a
  received handle request is rejected with `ERR`.

## Outer Frame

TCP requests continue to use `vemb_v16_net_hdr_t` with:

- `type = VEMB_V16_NET_REQUEST`
- `payload_len = exact encoded request payload bytes`

`hdr.flags` is no longer used to select among multiple TCP request encodings.
There is only one TCP request payload format.

## Base Header

All TCP request payloads start with a fixed 24-byte base header.

```c
typedef struct vemb_v16_tcp_req_base_hdr {
    uint8_t op_flags;
    uint8_t key_len;
    uint16_t dim;
    uint32_t req_id;
    uint64_t channel_id;
    uint64_t topology_epoch;
} vemb_v16_tcp_req_base_hdr_t;
```

Field semantics:

- `op_flags`
  - low 6 bits: request op
  - high 2 bits: common wire flags
- `key_len`
  - encoded key length in bytes
  - valid range `1..128` for key-bearing ops
- `dim`
  - vector dimension
  - `0` for `VREM`
- `req_id`
  - request identifier
- `channel_id`
  - target channel
- `topology_epoch`
  - client topology epoch

Recommended bit layout:

```c
#define VEMB_V16_TCP_REQ_OP_MASK   0x3fu
#define VEMB_V16_TCP_REQ_FLAG_MASK 0xc0u
```

## Per-Op Body Layout

The body layout is determined only by `op`.
Do not infer body shape from flags.

### `VEMB_INLINE`

```text
base | key[key_len]
```

Constraints:

- `key_len > 0`
- `dim > 0`

Note:

- Request side does not carry vector payload.
- Inline semantics apply to the response payload, not the request body.

### `VREM`

```text
base | key[key_len]
```

Constraints:

- `key_len > 0`
- `dim == 0`

### `VADD`

```text
base | vector_bytes:u32 | key[key_len] | vector[vector_bytes]
```

Constraints:

- `key_len > 0`
- `dim > 0`
- `vector_bytes == dim * sizeof(float)`

### `VSIM_INLINE`

```text
base | vector_bytes:u32 | key[key_len] | vector[vector_bytes]
```

Constraints:

- `key_len > 0`
- `dim > 0`
- `vector_bytes == dim * sizeof(float)`

### `VSIM_KEY_KEY`

```text
base | key2_len:u8 | key[key_len] | key2[key2_len]
```

Constraints:

- `key_len > 0`
- `key2_len > 0`
- `dim > 0`

## Encoding Rules

Scalar fields must use explicit field-by-field encoding with fixed byte order.

Examples:

- `op_flags`
- `key_len`
- `dim`
- `req_id`
- `channel_id`
- `topology_epoch`
- `vector_bytes`
- `key2_len`

Variable-length raw data should be copied directly with `memcpy`.

Examples:

- `key`
- `key2`
- `vector payload`

## Decoding Rules

`proxy` TCP receive path should:

1. Read `vemb_v16_net_hdr_t`.
1. Verify `type == VEMB_V16_NET_REQUEST`.
1. Read the 24-byte base header.
1. Extract `op` from `op_flags`.
1. Read the remaining body according to the op schema.
1. Fill a zeroed `vemb_v16_req_t`.
1. Reconstruct derived fields:
   - `req->op`
   - `req->flags` from common wire flag bits
   - `req->req_id`
   - `req->channel_id`
   - `req->topology_epoch`
   - `req->dim`
   - `req->key_len`
   - `req->key2_len` when applicable
   - `req->vector_bytes` when applicable
   - `req->key_hash = vemb_v16_murmur3(req->key, req->key_len)`
   - `req->key2_hash = vemb_v16_murmur3(req->key2, req->key2_len)` when applicable
1. Pass the reconstructed request into existing proxy validation/scheduling.

## Validation

Decode must reject:

- unknown `op`
- `key_len == 0` for key-bearing ops
- `key_len > VEMB_V16_MAX_KEY_LEN`
- `key2_len > VEMB_V16_MAX_KEY_LEN`
- `dim > VEMB_V16_MAX_DIM`
- `vector_bytes > sizeof(req->vector)`
- truncated payload
- extra trailing bytes after the expected body
- illegal op/body combinations

Examples of illegal combinations:

- `VREM` with `dim != 0`
- `VADD` without `vector_bytes`
- `VADD` where `vector_bytes != dim * sizeof(float)`
- `VSIM_KEY_KEY` without `key2_len`
- any `VEMB_HANDLE` request: handles are UB/AERON-only

## Expected Wire Size

Assume key `"item:12345"` with `key_len = 10`.

Outer TCP frame header remains `32B`.

Request payload sizes:

- `VREM`
  - `24 + 10 = 34B`
  - total frame size `66B`
- `VSIM_KEY_KEY`
  - `24 + 1 + 10 + key2_len`
  - with `key2_len = 10`: payload `45B`, total `77B`
- `VADD`
  - `24 + 4 + 10 + vector_bytes`
  - with `vector_bytes = 1200`: payload `1238B`, total `1270B`

## Suggested Implementation Points

- `src/vemb_v16_protocol.h`
  - add TCP request base-header constants and helpers
  - add exact encoded length / encode / decode helpers for the new request format
- `benchmark/vemb_v16_bench.c`
  - always encode TCP requests with this protocol
- `benchmark/vemb_v16_slow_client_bench.c`
  - same as above
- `src/vemb_v16_tcp_transport.c`
  - decode TCP requests with this protocol only

## Test Plan

Unit tests:

- roundtrip encode/decode for each op
- reject malformed body shapes
- reject truncated payloads
- reject trailing bytes

Integration tests:

- `VADD` over TCP
- reject `VEMB_HANDLE` over TCP
- `VEMB_INLINE` over TCP
- `VSIM_INLINE` over TCP
- `VSIM_KEY_KEY` over TCP
- `VREM` over TCP

Benchmark checks:

- compare request bytes/op before and after for each op
- measure CPU overhead of decode vs bandwidth savings
