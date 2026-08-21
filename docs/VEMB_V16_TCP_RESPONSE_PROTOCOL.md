# VEMB v16 TCP Response Protocol

## Goal

Define the compact TCP response payload format between `proxy` and `bench_cli`.

This protocol replaces the previous fixed-width TCP response layout entirely.

- Keep the existing outer `vemb_v16_net_hdr_t` frame.
- Replace the response payload with a compact status/op-specific encoding.
- Decode the compact TCP payload into internal `vemb_v16_resp_t` after receive.
- Keep SHM/Aeron response layout unchanged.

## Motivation

The old TCP response path always encoded the full `vemb_v16_resp_t` metadata,
which forced every response to carry fields such as:

- `key_hash`
- `vector_offset`
- `vector_bytes`
- `region_id`
- `local_slot`
- `owner_generation`
- `redirect_owner`
- `score`

Most ops do not need most of these fields on the wire.

This protocol makes TCP response size proportional to the actual result shape.

## Outer Frame

TCP responses continue to use `vemb_v16_net_hdr_t` with:

- `type = VEMB_V16_NET_RESPONSE`
- `payload_len = encoded response metadata bytes + optional inline vector bytes`

`hdr.flags` no longer selects among multiple response encodings.
There is only one compact TCP response payload format.

## Base Header

All TCP response payloads start with a fixed 6-byte base header.

```c
typedef struct vemb_v16_tcp_resp_base_hdr {
    uint8_t status;
    uint8_t op_flags;
    uint32_t req_id;
} vemb_v16_tcp_resp_base_hdr_t;
```

Field semantics:

- `status`
  - `VEMB_V16_STATUS_*`
- `op_flags`
  - low 6 bits: response op
  - high 2 bits: compact common flags
- `req_id`
  - request identifier

Recommended bit layout:

```c
#define VEMB_V16_TCP_RESP_OP_MASK   0x3fu
#define VEMB_V16_TCP_RESP_FLAG_MASK 0xc0u
```

## Per-Status And Per-Op Body Layout

The body layout is determined by `status`, then by `op`.

### `status == OK`

#### `PING`, `VADD`, `VREM`

```text
base
```

No extra metadata.

#### `VEMB_INLINE`

```text
base | vector_bytes:u32 | vector_offset:u64 | region_id:u32 | local_slot:u32 | owner_generation:u64
```

Notes:

- `VEMB_INLINE` inline payload bytes are still appended after encoded metadata.
- `dim` is reconstructed as `vector_bytes / sizeof(float)` after decode.

#### `VSIM_INLINE`, `VSIM_KEY_KEY`

```text
base | score:f32
```

Only score is returned on wire.

### `status == MOVED` or `status == ASK`

```text
base | redirect_owner:u32
```

### `status == NOT_FOUND`, `ERR`, `STALE_TOPOLOGY`

```text
base
```

No extra metadata.

## Encoding Rules

Scalar fields must use explicit field-by-field encoding with fixed byte order.

Variable-length raw data may be copied directly with `memcpy`.

The only raw payload appended after encoded metadata is:

- `VEMB_INLINE` success payload vector bytes

## Decoding Rules

`bench_cli` TCP receive path should:

1. Read `vemb_v16_net_hdr_t`.
1. Verify `type == VEMB_V16_NET_RESPONSE`.
1. Read the 6-byte base header.
1. Extract `status` and `op` from the base header.
1. Compute encoded metadata length from `status/op`.
1. Read the rest of the metadata body.
1. Decode into a zeroed `vemb_v16_resp_t`.
1. If payload bytes remain after metadata:
   - treat them as inline vector bytes
   - this is valid only for `VEMB_INLINE` success responses
1. Reconstruct derived fields:
   - `resp->flags` from compact wire flag bits
   - `resp->dim = resp->vector_bytes / sizeof(float)` for handle-style success responses

## Expected Metadata Size

Encoded response metadata sizes:

- base-only ack/error
  - `6B`
- redirect response
  - `10B`
- `VSIM_*` success
  - `10B`
- `VEMB_INLINE` success metadata
  - `34B`
  - plus appended inline vector payload bytes

Compared with the previous fixed `56B` metadata response:

- ack/error saves `50B`
- redirect saves `46B`
- `VSIM_*` saves `46B`
- `VEMB_INLINE` metadata saves `22B`
