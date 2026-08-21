#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTL="$ROOT/benchmark/vemb_v16_topology_ctl"
BASE_PORT="${VEMB_V16_TOPOLOGY_COORD_SMOKE_BASE_PORT:-$((28600 + $$ % 3000))}"

printf '[build] benchmark/vemb_v16_topology_ctl\n'
make -C "$ROOT/benchmark" vemb_v16_topology_ctl >/dev/null

python3 - "$CTL" "$BASE_PORT" <<'PY'
import socket
import struct
import subprocess
import sys
import threading
import time

CTL = sys.argv[1]
BASE_PORT = int(sys.argv[2])
COORD_PORT = BASE_PORT
TARGET_PORTS = [BASE_PORT + 1, BASE_PORT + 2]

MAGIC = 0x56313645
VERSION = 1
NET_TOPOLOGY_SET = 0x12
NET_TOPOLOGY_RESPONSE = 0x14
NET_SCALEOUT_LOCAL_DONE = 0x1B
NET_SCALEOUT_LOCAL_DONE_RESPONSE = 0x1C
STATUS_OK = 0
TOPOLOGY_REQ_SIZE = 424
TOPOLOGY_RESP_SIZE = 41
SCALEOUT_LOCAL_DONE_REQ_SIZE = 56
SCALEOUT_LOCAL_DONE_RESP_SIZE = 29

hits = []
errors = []


def read_full(sock, n):
    chunks = []
    remain = n
    while remain:
        chunk = sock.recv(remain)
        if not chunk:
            raise RuntimeError("connection closed")
        chunks.append(chunk)
        remain -= len(chunk)
    return b"".join(chunks)


def frame(frame_type, payload):
    hdr = struct.pack(
        "<IHHIIQII",
        MAGIC,
        VERSION,
        frame_type,
        0,
        len(payload),
        0,
        0,
        0,
    )
    return hdr + payload


def topology_resp():
    return frame(NET_TOPOLOGY_RESPONSE, bytes([STATUS_OK]) + bytes(TOPOLOGY_RESP_SIZE - 1))


def serve_one(owner, port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(1)
    srv.settimeout(10)
    try:
        conn, _ = srv.accept()
        with conn:
            hdr = read_full(conn, 32)
            magic, version, frame_type, _flags, payload_len, _ch, _req_id, _reserved = struct.unpack(
                "<IHHIIQII", hdr
            )
            payload = read_full(conn, payload_len)
            if magic != MAGIC or version != VERSION:
                raise RuntimeError("bad topology frame header")
            epoch = struct.unpack_from(">Q", payload, 0)[0]
            active_count = struct.unpack_from(">I", payload, 16)[0]
            standby_count = struct.unpack_from(">I", payload, 20)[0]
            endpoint_count = struct.unpack_from(">I", payload, 48)[0]
            hits.append(
                {
                    "owner": owner,
                    "type": frame_type,
                    "payload_len": payload_len,
                    "epoch": epoch,
                    "active_count": active_count,
                    "standby_count": standby_count,
                    "endpoint_count": endpoint_count,
                }
            )
            conn.sendall(topology_resp())
    except Exception as exc:
        errors.append(f"target {owner}: {exc}")
    finally:
        srv.close()


threads = []
for owner, port in enumerate(TARGET_PORTS):
    t = threading.Thread(target=serve_one, args=(owner, port), daemon=True)
    t.start()
    threads.append(t)

cmd = [
    CTL,
    "--coordinator-listen",
    "--transport",
    "tcp",
    "--host",
    "127.0.0.1",
    "--port",
    str(COORD_PORT),
    "--expected-sources",
    "0",
    "--migration-epoch",
    "23",
    "--cutover-epoch",
    "24",
    "--standby",
    "0,1",
    "--owner-endpoints",
    f"0=127.0.0.1:{TARGET_PORTS[0]},1=127.0.0.1:{TARGET_PORTS[1]}",
    "--wait-ms",
    "10000",
    "--timeout-ms",
    "2000",
]
proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

deadline = time.time() + 5
while time.time() < deadline:
    try:
        probe = socket.create_connection(("127.0.0.1", COORD_PORT), timeout=0.1)
        probe.close()
        break
    except OSError:
        time.sleep(0.05)
else:
    proc.kill()
    out, err = proc.communicate()
    raise SystemExit(f"coordinator did not listen\nstdout:\n{out}\nstderr:\n{err}")

payload = struct.pack(
    ">QQQIIIIIIII",
    23,
    24,
    23,
    0,
    4,
    0,
    0,
    0,
    0,
    0,
    0,
)
assert len(payload) == SCALEOUT_LOCAL_DONE_REQ_SIZE

with socket.create_connection(("127.0.0.1", COORD_PORT), timeout=2) as sock:
    sock.sendall(frame(NET_SCALEOUT_LOCAL_DONE, payload))
    hdr = read_full(sock, 32)
    magic, version, frame_type, _flags, payload_len, _ch, _req_id, _reserved = struct.unpack(
        "<IHHIIQII", hdr
    )
    resp = read_full(sock, payload_len)
    if (
        magic != MAGIC
        or version != VERSION
        or frame_type != NET_SCALEOUT_LOCAL_DONE_RESPONSE
        or payload_len != SCALEOUT_LOCAL_DONE_RESP_SIZE
        or resp[0] != STATUS_OK
    ):
        raise SystemExit(
            "coordinator callback ack was not OK: "
            f"magic={magic:#x} version={version} type={frame_type:#x} "
            f"payload_len={payload_len} status={resp[0] if resp else None}"
        )

out, err = proc.communicate(timeout=15)
for t in threads:
    t.join(timeout=2)

if proc.returncode != 0:
    raise SystemExit(f"coordinator failed rc={proc.returncode}\nstdout:\n{out}\nstderr:\n{err}")
if errors:
    raise SystemExit("fake topology target errors: " + "; ".join(errors))
if len(hits) != 2:
    raise SystemExit(f"expected 2 topology publishes, got {len(hits)} hits={hits}")
for hit in hits:
    if (
        hit["type"] != NET_TOPOLOGY_SET
        or hit["payload_len"] != TOPOLOGY_REQ_SIZE
        or hit["epoch"] != 24
        or hit["active_count"] != 2
        or hit["standby_count"] != 2
        or hit["endpoint_count"] != 2
    ):
        raise SystemExit(f"unexpected topology publish payload: {hit}")
if "scaleout_full_active_published=2 errors=0 targets=2" not in out:
    raise SystemExit(f"missing publish success line\nstdout:\n{out}")

print("[ok] topology_ctl coordinator smoke passed")
PY
