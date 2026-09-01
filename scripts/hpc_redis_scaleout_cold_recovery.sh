#!/usr/bin/env bash
# End-to-end TCP/COLD smoke test for the two-node VEMB deployment.
#
# The Redis integration has no external checkpoint/compact command.  Therefore
# live write/read/restart recovery use redis-server + vemb_v16_bench, while the
# compact phase runs the repository's tlc_cold_ut against the same COLD build.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
NODE0_HOST="${NODE0_HOST:-192.168.1.111}"
NODE1_HOST="${NODE1_HOST:-192.168.1.112}"
SSH_USER="${SSH_USER:-root}"
NODE0_SSH_HOST="${NODE0_SSH_HOST:-43.154.145.18}"
NODE1_SSH_HOST="${NODE1_SSH_HOST:-43.154.145.18}"
NODE0_SSH_PORT="${NODE0_SSH_PORT:-8111}"
NODE1_SSH_PORT="${NODE1_SSH_PORT:-8112}"
REMOTE_DIR="${REMOTE_DIR:-/root/szz/codespace/hpc-redis}"
PORT="${PORT:-6391}"
DIM="${DIM:-16}"
MAX_VECTORS="${MAX_VECTORS:-8192}"
PREFILL_KEYS="${PREFILL_KEYS:-1024}"
READ_OPS="${READ_OPS:-${PREFILL_KEYS}}"
WRITE_THREADS="${WRITE_THREADS:-1}"
REQUEST_TIMEOUT_MS="${REQUEST_TIMEOUT_MS:-120000}"
RUN_ID="${RUN_ID:-$(date +%Y%m%d_%H%M%S)}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT_DIR/benchmark/results/scaleout}"
LOCAL_RUN_DIR="$RESULTS_DIR/$RUN_ID"
REMOTE_SUBDIR="benchmark/results/scaleout/$RUN_ID/cold_recovery"
REMOTE_RESULT_DIR="$REMOTE_DIR/$REMOTE_SUBDIR"
TSV="$LOCAL_RUN_DIR/summary.tsv"

mkdir -p "$LOCAL_RUN_DIR"
printf 'phase\tstatus\tdetail\n' >"$TSV"

ssh_run() {
    local host=$1
    shift
    local ssh_host=$host
    local ssh_port=${SSH_DIRECT_PORT:-22}
    if [ "$host" = "$NODE0_HOST" ]; then
        ssh_host=$NODE0_SSH_HOST
        ssh_port=$NODE0_SSH_PORT
    elif [ "$host" = "$NODE1_HOST" ]; then
        ssh_host=$NODE1_SSH_HOST
        ssh_port=$NODE1_SSH_PORT
    fi
    ssh -p "$ssh_port" "$SSH_USER@$ssh_host" "$@"
}
record() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" | tee -a "$TSV"; }
die() { echo "ERROR: $*" >&2; exit 1; }

hosts=("$NODE0_HOST" "$NODE1_HOST")
manifests=("$REMOTE_RESULT_DIR/node0.manifest" "$REMOTE_RESULT_DIR/node1.manifest")
logs=("$REMOTE_RESULT_DIR/server_node0.log" "$REMOTE_RESULT_DIR/server_node1.log")
cold_dirs=("$REMOTE_RESULT_DIR/cold_node0" "$REMOTE_RESULT_DIR/cold_node1")
region_names=("/v16_cold_region_0" "/v16_cold_region_1")

stop_node() {
    local host=$1
    ssh_run "$host" "pids=\$(pgrep -f '$REMOTE_DIR/src/redis-server 0.0.0.0:$PORT' || true); [ -z \"\$pids\" ] || kill -TERM \$pids 2>/dev/null || true; sleep 1; pids=\$(pgrep -f '$REMOTE_DIR/src/redis-server 0.0.0.0:$PORT' || true); [ -z \"\$pids\" ] || kill -KILL \$pids 2>/dev/null || true" || true
}

wait_ready() {
    local host=$1
    for _ in $(seq 1 100); do
        if ssh_run "$host" "bash -c '</dev/tcp/127.0.0.1/$PORT'" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

cleanup() {
    stop_node "${NODE0_HOST}" || true
    stop_node "${NODE1_HOST}" || true
}
trap cleanup EXIT

echo "== remote prerequisites and build =="
for host in "${hosts[@]}"; do
    ssh_run "$host" "test -d '$REMOTE_DIR' && mkdir -p '$REMOTE_RESULT_DIR'" || die "cannot access $host:$REMOTE_DIR"
    ssh_run "$host" "cd '$REMOTE_DIR' && make -C src vemb_v16_server && make -C benchmark vemb_v16_bench tlc_cold_ut" \
        >"$LOCAL_RUN_DIR/build_${host}.log"
done
record build PASS "server, vemb_v16_bench, tlc_cold_ut built on both nodes"

echo "== manifests and clean start =="
# The warm layout stores slot metadata alongside vectors; reserve 2x payload
# bytes so the usable slot count is not reduced by the allocator header.
region_bytes=$((DIM * 4 * MAX_VECTORS * 2))
for i in 0 1; do
    host=${hosts[$i]}; manifest=${manifests[$i]}; cold=${cold_dirs[$i]}; region=${region_names[$i]}; log_path=${logs[$i]}
    stop_node "$host"
    ssh_run "$host" "rm -rf '$cold'; mkdir -p '$cold'; cat >'$manifest' <<EOF
local_region_weight: 4
warm_regions:
  - region_id: 0
    provider: shm
    path: $region
    is_local: true
    cache_policy: cacheable
    mmap_offset: 0
    bytes: $region_bytes
    value_size: $((DIM * 4))
    home_ub_node_id: $i
EOF
HPC_REDIS_COLD_DIR='$cold' '$REMOTE_DIR/src/redis-server' --port '$PORT' --bind 0.0.0.0 --protected-mode no --save '' --appendonly no --vemb-v16-enabled yes --vemb-v16-dim '$DIM' --vemb-v16-max-vectors '$MAX_VECTORS' --vemb-v16-warm-regions-manifest '$manifest' --vemb-v16-reset-warm-regions yes --vemb-v16-proxy-io-threads 2 --vemb-v16-supernode-workers 2 --daemonize yes --logfile '$log_path' --loglevel notice"
    wait_ready "$host" || exit 1
done
record start PASS "two Redis/VEMB TCP endpoints listening on port $PORT"

echo "== write =="
write_out="$REMOTE_RESULT_DIR/write.out"
ssh_run "${NODE0_HOST}" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_bench --transport tcp --endpoints '${NODE0_HOST}:$PORT,${NODE1_HOST}:$PORT' --dim '$DIM' --prefill 0 --keyspace '$PREFILL_KEYS' --ops '$PREFILL_KEYS' --threads '$WRITE_THREADS' --pipeline 1 --mode vadd --timeout-ms '$REQUEST_TIMEOUT_MS' >'$write_out' 2>&1"
ssh_run "${NODE0_HOST}" "grep -q '\[done\].*fail=0' '$write_out'"
record write PASS "keys=$PREFILL_KEYS; artifact=$write_out"

echo "== read =="
read_out="$REMOTE_RESULT_DIR/read.out"
ssh_run "${NODE0_HOST}" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_bench --transport tcp --endpoints '${NODE0_HOST}:$PORT,${NODE1_HOST}:$PORT' --dim '$DIM' --prefill 0 --keyspace '$PREFILL_KEYS' --ops '$READ_OPS' --threads 1 --pipeline 1 --mode vemb-inline --timeout-ms '$REQUEST_TIMEOUT_MS' >'$read_out' 2>&1"
ssh_run "${NODE0_HOST}" "grep -q '\[done\].*fail=0' '$read_out'"
record read PASS "ops=$READ_OPS; artifact=$read_out"

echo "== compact =="
for host in "${hosts[@]}"; do
    compact_out="$REMOTE_RESULT_DIR/compact.out"
    ssh_run "$host" "cd '$REMOTE_DIR' && ./benchmark/tlc_cold_ut >'$compact_out' 2>&1"
    ssh_run "$host" "grep -q 'PASS' '$compact_out'"
done
record compact PASS "tlc_cold_ut checkpoint/compact interruption and retention cases passed"

echo "== restart and recovery read =="
for host in "${hosts[@]}"; do stop_node "$host"; done
for i in 0 1; do
    host=${hosts[$i]}; manifest=${manifests[$i]}; cold=${cold_dirs[$i]}; log_path=${logs[$i]}
    ssh_run "$host" "HPC_REDIS_COLD_DIR='$cold' '$REMOTE_DIR/src/redis-server' --port '$PORT' --bind 0.0.0.0 --protected-mode no --save '' --appendonly no --vemb-v16-enabled yes --vemb-v16-dim '$DIM' --vemb-v16-max-vectors '$MAX_VECTORS' --vemb-v16-warm-regions-manifest '$manifest' --vemb-v16-proxy-io-threads 2 --vemb-v16-supernode-workers 2 --daemonize yes --logfile '$log_path' --loglevel notice"
    wait_ready "$host" || exit 1
done
recovery_out="$REMOTE_RESULT_DIR/recovery_read.out"
ssh_run "${NODE0_HOST}" "cd '$REMOTE_DIR' && ./benchmark/vemb_v16_bench --transport tcp --endpoints '${NODE0_HOST}:$PORT,${NODE1_HOST}:$PORT' --dim '$DIM' --prefill 0 --keyspace '$PREFILL_KEYS' --ops '$READ_OPS' --threads 1 --pipeline 1 --mode vemb-inline --timeout-ms '$REQUEST_TIMEOUT_MS' >'$recovery_out' 2>&1"
ssh_run "${NODE0_HOST}" "grep -q '\[done\].*fail=0' '$recovery_out'"
record recovery PASS "post-restart read ops=$READ_OPS; artifact=$recovery_out"

printf 'DONE: %s\n' "$TSV"
cat "$TSV"
