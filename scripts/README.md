NODE=192.168.90.112 BUILD=1 WORKERS=20:20  REMOTE_DIR=/root/szz/codespace/hpc-redis_bench  bash scripts/run_host_mt_server_flamegraph.sh

NODE=192.168.90.112 bash scripts/sync_changed_code_to_peer.sh --dry-run

Cross-node VEMB tests must synchronize and rebuild before starting either role:

```sh
NODE=192.168.90.111 bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build server --verify-build server
NODE=192.168.90.112 bash scripts/sync_changed_code_to_peer.sh \
  --all-code --build client --verify-build client
```

`vemb_v16_build_stamp.sh` records source, build-setting, build-policy, and
artifact hashes. The policy is server and client SDK/memtier `-O3 -flto` plus
SVE. After an equivalent manual remote
build, run `bash scripts/vemb_v16_build_stamp.sh write server|client`;
`run_aeron_best.sh` verifies the matching stamp by default.

## Cross-node VEMB flamegraphs

`run_aeron_cross_node_flamegraph.sh` starts a fresh VEMB v16 server on 111,
prefills sequential VADD keys from 112, runs a real V2 `VEMB_HANDLE` read
workload under `perf`, and pulls complete server/client artifacts into `perf/`.
It captures every server TID, so the merged server flamegraph includes both
proxy IO and pooled SuperNode stacks, with user and kernel frames.

Run the default 100k uniform workload:

```sh
bash scripts/run_aeron_cross_node_flamegraph.sh
```

Run the 100k Zipf 1.5 hot-key workload:

```sh
KEY_PATTERN=Z:Z ZIPF_S=1.5 \
  bash scripts/run_aeron_cross_node_flamegraph.sh
```

Run the 10k Zipf 1.5 workload:

```sh
NUM_KEYS=10000 KEY_PATTERN=Z:Z ZIPF_S=1.5 \
  bash scripts/run_aeron_cross_node_flamegraph.sh
```

Validate the configuration without SSH, build, benchmark, or sampling:

```sh
DRY_RUN=1 KEY_PATTERN=Z:Z ZIPF_S=1.5 \
  bash scripts/run_aeron_cross_node_flamegraph.sh
```

Important parameters:

| Variable | Default | Meaning |
|---|---:|---|
| `NUM_KEYS` | `100000` | Prefilled/read keyspace size. |
| `KEY_PATTERN` | `R:R` | Read key pattern: `R:R` uniform random, `S:S` sequential, or `Z:Z` Zipf. |
| `ZIPF_S` | none | Positive Zipf exponent; required for `KEY_PATTERN=Z:Z`. |
| `TEST_TIME` | `30` | Client read workload duration in seconds. |
| `SERVER_FLAME_DURATION` | `25` | Server `perf` duration; must be less than `TEST_TIME`. |
| `THREADS`, `CLIENTS` | `64`, `4` | memtier worker threads and clients per thread. |
| `PIPELINE` | `32` | Client pipeline depth. |
| `BATCH_REQUEST_SIZE` | `32` | VEMB v2 request batch size on both client and server. |
| `BATCH_MAX_DELAY_US` | `0` | Client batch deadline; zero disables delay-based flushing. |
| `PIO`, `SNW` | `21`, `21` | Server proxy-IO and SuperNode worker counts. |
| `SERVER_CPU_MASK`, `CLIENT_CPU_MASK` | `0-47`, `96-191` | CPU sets used by server and client. |
| `BUILD` | `verify` | `verify` requires matching O3/LTO/SVE build stamps; `build` rebuilds the respective remote role first. |
| `KEEP_SERVER` | `0` | Set `1` to keep the temporary server running after the run. |
| `MAX_FOREIGN_CPU_PCT` | `10` | Fail before startup if any existing process consumes more CPU than this percentage in a 1-second `pidstat` sample. |
| `MAX_FOREIGN_TOTAL_CPU_PCT` | `20` | Fail before startup if all existing processes together exceed this CPU percentage. |
| `MAX_FOREIGN_RSS_MB` | `256` | Fail before startup if an existing process RSS exceeds this MiB limit. |
| `KILL_OPENCODE` | `1` | Set `0` to preserve the `opencode` tmux session and exact-name `opencode` processes before the load gate. |
| `RUN_ID`, `LOCAL_ROOT` | timestamp | Artifact name and local output directory. |

The CPU/RSS gate is mandatory on both 111 and 112 before the server, prefill,
or benchmark starts. It reports every blocking PID, CPU percentage, RSS, and
command; clear the unrelated workload rather than bypassing the gate. Its raw
`pidstat` and blocker files are retained in the successful server/client
archives.

By default the runner stops the `opencode` tmux session before removing any
residual exact-name process. It also removes each `mutagen-agent` direct parent
before removing the agent, preventing its immediate respawn. Set
`KILL_OPENCODE=0` or `KILL_MUTAGEN=0` only to retain the respective workload.

Each SVG title carries the compact run label: key count and distribution,
dimension, `PIO`/`SNW`, threads/clients, pipeline, batch size, batch delay,
test and flame durations, and `RUN_ID`. The subtitle identifies the role,
CPU mask, event, and sample frequency.

The server archive contains `server.svg`, raw `perf.data`/`perf.script`,
folded stacks, logs, and server-only CPU statistics. `server.cpu.process.tsv`
contains Redis user/system/total equivalent cores for the fixed `TEST_TIME`
window. `server.cpu.cpuset.mpstat.txt` and `.summary.tsv` report the pinned
server CPU set's `usr`, `sys`, `irq`, `soft` (`si`), `iowait`, and
`total=100-idle` usage. `server.cpu.summary.txt` is the aligned human-readable
table printed at the end of a successful run. Client CPU usage is not sampled.
