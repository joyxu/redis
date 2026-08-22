#!/usr/bin/env bash

set -euo pipefail

HOST01="${HOST01:-192.168.1.111}"
HOST2="${HOST2:-192.168.1.112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_DIR="${REMOTE_DIR:-/root/szz/codespace/hpc-redis}"

OWNER0_PORT="${OWNER0_PORT:-6391}"
OWNER1_PORT="${OWNER1_PORT:-6392}"
OWNER2_PORT="${OWNER2_PORT:-6391}"
COORD_PORT="${COORD_PORT:-7391}"

DIM="${DIM:-16}"
MAX_VECTORS="${MAX_VECTORS:-8192}"
PREFILL_KEYS="${PREFILL_KEYS:-1024}"
POST_KEYSPACE="${POST_KEYSPACE:-2000}"
POST_OPS="${POST_OPS:-2000}"
POST_THREADS="${POST_THREADS:-2}"
LIVE_WRITE_OPS="${LIVE_WRITE_OPS:-50000}"
LIVE_MODE="${LIVE_MODE:-vadd}"
LIVE_THREADS="${LIVE_THREADS:-2}"
LIVE_TIMEOUT_MS="${LIVE_TIMEOUT_MS:-180000}"
MIGRATION_EPOCH="${MIGRATION_EPOCH:-23}"
CUTOVER_EPOCH="${CUTOVER_EPOCH:-24}"

RUN_LIVE_WRITE="${RUN_LIVE_WRITE:-0}"
RUN_POST_READ="${RUN_POST_READ:-1}"
VERIFY_MIGRATED_DATA="${VERIFY_MIGRATED_DATA:-1}"
VERIFY_SAMPLE_COUNT="${VERIFY_SAMPLE_COUNT:-16}"

OWNER0_MANIFEST="${OWNER0_MANIFEST:-/tmp/v16_owner0.yaml}"
OWNER1_MANIFEST="${OWNER1_MANIFEST:-/tmp/v16_owner1.yaml}"
OWNER2_MANIFEST="${OWNER2_MANIFEST:-/tmp/v16_owner2.yaml}"
OWNER0_LOG="${OWNER0_LOG:-/tmp/v16_owner0.log}"
OWNER1_LOG="${OWNER1_LOG:-/tmp/v16_owner1.log}"
OWNER2_LOG="${OWNER2_LOG:-/tmp/v16_owner2.log}"
PREFILL_OUT="${PREFILL_OUT:-/tmp/v16_prefill_3owner.out}"
LIVE_WRITE_OUT="${LIVE_WRITE_OUT:-/tmp/v16_live_write_3owner.out}"
LIVE_WRITE_PID="${LIVE_WRITE_PID:-/tmp/v16_live_write_3owner.pid}"
LIVE_WRITE_RC="${LIVE_WRITE_RC:-/tmp/v16_live_write_3owner.rc}"
COORD_OUT="${COORD_OUT:-/tmp/v16_coordinator_3owner.out}"
COORD_ERR="${COORD_ERR:-/tmp/v16_coordinator_3owner.err}"
POST_WRITE_OUT="${POST_WRITE_OUT:-/tmp/v16_post_write_3owner.out}"
POST_READ_OUT="${POST_READ_OUT:-/tmp/v16_post_read_3owner.out}"

RESET_OWNER0="${RESET_OWNER0:-1}"
RESET_OWNER1="${RESET_OWNER1:-0}"
RESET_OWNER2="${RESET_OWNER2:-0}"

ssh_run() {
    local host="$1"
    shift
    ssh -p 22 "${SSH_USER}@${host}" "$@"
}

step() {
    printf '\n== %s ==\n' "$1"
}

remote_kill_vemb_processes() {
    local host="$1"
    ssh_run "${host}" "pkill -f '(^|/)(vemb_v16_server|vemb_v16_topology_ctl|vemb_v16_bench)( |$)' || true"
}

require_remote_file() {
    local host="$1"
    local path="$2"
    ssh_run "${host}" "test -f ${path}"
}

wait_for_topology() {
    local host="$1"
    local port="$2"
    local out="$3"
    ssh_run "${host}" "cd ${REMOTE_DIR} && for _ in \$(seq 1 80); do if ./benchmark/vemb_v16_topology_ctl --get --transport tcp --host 127.0.0.1 --port ${port} --timeout-ms 1000 >${out} 2>&1; then exit 0; fi; sleep 0.25; done; exit 1"
}

start_owner() {
    local host="$1"
    local port="$2"
    local manifest="$3"
    local log="$4"
    local reset="$5"
    local reset_arg=""
    if [[ "$reset" == "1" ]]; then
        reset_arg="--reset-warm-regions"
    fi
    ssh_run "${host}" "cd ${REMOTE_DIR} && setsid -f ./src/vemb_v16_server --transport tcp --tcp-host ${host} --tcp-port ${port} --proxy-io-threads 1 --supernode-workers 1 --warm-regions-manifest ${manifest} ${reset_arg} --dim ${DIM} --max-vectors ${MAX_VECTORS} --loglevel notice >${log} 2>&1 </dev/null"
}

assert_topology() {
    local host="$1"
    local port="$2"
    local expected_epoch="$3"
    local expected_active="$4"
    local expected_standby="$5"
    local expected_count="$6"
    local out="$7"
    ssh_run "${host}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --get --transport tcp --host ${host} --port ${port} --timeout-ms 5000 >${out} && grep -q '^status=0$' ${out} && grep -q '^current_topology_epoch=${expected_epoch}$' ${out} && grep -q '^min_write_epoch=${expected_epoch}$' ${out} && grep -q '^active_owners=${expected_active}$' ${out} && grep -q '^standby_owners=${expected_standby}$' ${out} && grep -q '^endpoint_count=${expected_count}$' ${out}"
}

assert_source_migrated() {
    local host="$1"
    local owner="$2"
    local log="$3"
    ssh_run "${host}" "grep -Eq 'vemb_v16 migration auto plan: local_owner=${owner} .*marked=[1-9]' ${log}"
    ssh_run "${host}" "grep -q 'vemb_v16 scaleout auto done: local_owner=${owner} migration_epoch=${MIGRATION_EPOCH} cutover_epoch=${CUTOVER_EPOCH}' ${log} || grep -q 'vemb_v16 scaleout auto local done: local_owner=${owner} migration_epoch=${MIGRATION_EPOCH} cutover_epoch=${CUTOVER_EPOCH}' ${log}"
}

owner_endpoints_all() {
    printf '0=%s:%s,1=%s:%s,2=%s:%s' \
        "${HOST01}" "${OWNER0_PORT}" \
        "${HOST01}" "${OWNER1_PORT}" \
        "${HOST2}" "${OWNER2_PORT}"
}

owner_endpoints_initial() {
    printf '0=%s:%s,1=%s:%s' \
        "${HOST01}" "${OWNER0_PORT}" \
        "${HOST01}" "${OWNER1_PORT}"
}

step "Check remote manifests"
require_remote_file "${HOST01}" "${OWNER0_MANIFEST}"
require_remote_file "${HOST01}" "${OWNER1_MANIFEST}"
require_remote_file "${HOST2}" "${OWNER2_MANIFEST}"

step "Build binaries on both hosts"
ssh_run "${HOST01}" "cd ${REMOTE_DIR} && make -C src vemb_v16_server && make -C benchmark vemb_v16_bench && make -C benchmark vemb_v16_topology_ctl && make -C benchmark vemb_v16_read_verify"
ssh_run "${HOST2}" "cd ${REMOTE_DIR} && make -C src vemb_v16_server && make -C benchmark vemb_v16_bench && make -C benchmark vemb_v16_topology_ctl && make -C benchmark vemb_v16_read_verify"

step "Stop old processes"
remote_kill_vemb_processes "${HOST01}"
remote_kill_vemb_processes "${HOST2}"

step "Clean old outputs"
ssh_run "${HOST01}" "rm -f ${OWNER0_LOG} ${OWNER1_LOG} ${PREFILL_OUT} ${LIVE_WRITE_OUT} ${LIVE_WRITE_PID} ${LIVE_WRITE_RC} ${COORD_OUT} ${COORD_ERR} ${POST_WRITE_OUT} ${POST_READ_OUT}"
ssh_run "${HOST2}" "rm -f ${OWNER2_LOG}"

step "Start owner0 on host01"
start_owner "${HOST01}" "${OWNER0_PORT}" "${OWNER0_MANIFEST}" "${OWNER0_LOG}" "${RESET_OWNER0}"

step "Start owner1 on host01"
start_owner "${HOST01}" "${OWNER1_PORT}" "${OWNER1_MANIFEST}" "${OWNER1_LOG}" "${RESET_OWNER1}"

step "Start owner2 on host2"
start_owner "${HOST2}" "${OWNER2_PORT}" "${OWNER2_MANIFEST}" "${OWNER2_LOG}" "${RESET_OWNER2}"

sleep 2

step "Wait for all owners"
wait_for_topology "${HOST01}" "${OWNER0_PORT}" "/tmp/v16_owner0_ready.out"
wait_for_topology "${HOST01}" "${OWNER1_PORT}" "/tmp/v16_owner1_ready.out"
wait_for_topology "${HOST2}" "${OWNER2_PORT}" "/tmp/v16_owner2_ready.out"

step "Verify startup logs"
ssh_run "${HOST01}" "grep -E 'remote meta ready|registered vemb_v16 remote meta owner view|ub rpc ready|server ready' ${OWNER0_LOG}"
ssh_run "${HOST01}" "grep -E 'remote meta ready|registered vemb_v16 remote meta owner view|ub rpc ready|server ready' ${OWNER1_LOG}"
ssh_run "${HOST2}" "grep -E 'remote meta ready|registered vemb_v16 remote meta owner view|ub rpc ready|server ready' ${OWNER2_LOG}"

step "Publish initial topology active={0,1}"
ssh_run "${HOST01}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${HOST01} --port ${OWNER0_PORT} --epoch 1 --min-write-epoch 1 --active 0,1 --standby 0,1 --owner-endpoints $(owner_endpoints_initial) --timeout-ms 5000"
ssh_run "${HOST01}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${HOST01} --port ${OWNER1_PORT} --epoch 1 --min-write-epoch 1 --active 0,1 --standby 0,1 --owner-endpoints $(owner_endpoints_initial) --timeout-ms 5000"

step "Prefill initial dataset"
ssh_run "${HOST01}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${HOST01}:${OWNER0_PORT} --dim ${DIM} --prefill ${PREFILL_KEYS} --ops 0 --threads 1 --pipeline 1 --mode vadd --timeout-ms 10000 >${PREFILL_OUT} 2>&1 && cat ${PREFILL_OUT}"

if [[ "${RUN_LIVE_WRITE}" == "1" ]]; then
    step "Start live write pressure during scaleout"
    ssh_run "${HOST01}" "cd ${REMOTE_DIR} && setsid -f sh -lc 'echo \$\$ >${LIVE_WRITE_PID}; ./benchmark/vemb_v16_bench --transport tcp --endpoints ${HOST01}:${OWNER0_PORT} --dim ${DIM} --prefill 0 --keyspace ${PREFILL_KEYS} --ops ${LIVE_WRITE_OPS} --threads ${LIVE_THREADS} --pipeline 1 --mode ${LIVE_MODE} --timeout-ms ${LIVE_TIMEOUT_MS} >${LIVE_WRITE_OUT} 2>&1 </dev/null; rc=\$?; echo \$rc >${LIVE_WRITE_RC}; exit \$rc'"
fi

step "Start coordinator listener"
ssh_run "${HOST01}" "cd ${REMOTE_DIR} && setsid -f ./benchmark/vemb_v16_topology_ctl --coordinator-listen --transport tcp --host ${HOST01} --port ${COORD_PORT} --expected-sources 0,1 --migration-epoch ${MIGRATION_EPOCH} --cutover-epoch ${CUTOVER_EPOCH} --standby 0,1,2 --owner-endpoints $(owner_endpoints_all) --wait-ms 60000 --timeout-ms 5000 >${COORD_OUT} 2>${COORD_ERR} </dev/null"
sleep 1

step "Publish candidate topology to owner2 then owner0/owner1"
ssh_run "${HOST2}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${HOST2} --port ${OWNER2_PORT} --epoch ${MIGRATION_EPOCH} --min-write-epoch ${MIGRATION_EPOCH} --active 0,1 --standby 0,1,2 --dual-write --auto-scaleout --coordinated-scaleout --owner-endpoints $(owner_endpoints_all) --coordinator-endpoint ${HOST01}:${COORD_PORT} --timeout-ms 5000"
ssh_run "${HOST01}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${HOST01} --port ${OWNER0_PORT} --epoch ${MIGRATION_EPOCH} --min-write-epoch ${MIGRATION_EPOCH} --active 0,1 --standby 0,1,2 --dual-write --auto-scaleout --coordinated-scaleout --owner-endpoints $(owner_endpoints_all) --coordinator-endpoint ${HOST01}:${COORD_PORT} --timeout-ms 5000"
ssh_run "${HOST01}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_topology_ctl --set --transport tcp --host ${HOST01} --port ${OWNER1_PORT} --epoch ${MIGRATION_EPOCH} --min-write-epoch ${MIGRATION_EPOCH} --active 0,1 --standby 0,1,2 --dual-write --auto-scaleout --coordinated-scaleout --owner-endpoints $(owner_endpoints_all) --coordinator-endpoint ${HOST01}:${COORD_PORT} --timeout-ms 5000"

step "Show coordinator result"
ssh_run "${HOST01}" "for _ in \$(seq 1 60); do if grep -q '^scaleout_all_sources_done=2$' ${COORD_OUT} && grep -q '^scaleout_full_active_published=3 errors=0 targets=3$' ${COORD_OUT}; then exit 0; fi; sleep 1; done; exit 1"
ssh_run "${HOST01}" "cat ${COORD_OUT}; echo '---'; cat ${COORD_ERR} || true"

if [[ "${RUN_LIVE_WRITE}" == "1" ]]; then
    step "Wait live write pressure result"
    ssh_run "${HOST01}" "pid=\$(cat ${LIVE_WRITE_PID}); for _ in \$(seq 1 240); do if ! kill -0 \$pid 2>/dev/null; then break; fi; sleep 1; done; if kill -0 \$pid 2>/dev/null; then echo 'live write still running'; exit 1; fi; grep -q '^\\[done\\]' ${LIVE_WRITE_OUT}; test \"\$(cat ${LIVE_WRITE_RC})\" = \"0\""
    ssh_run "${HOST01}" "cat ${LIVE_WRITE_OUT}"
fi

step "Show source migration summary"
ssh_run "${HOST01}" "grep -E 'migration auto plan|scaleout auto|local done' ${OWNER0_LOG} | tail -n 20"
ssh_run "${HOST01}" "grep -E 'migration auto plan|scaleout auto|local done' ${OWNER1_LOG} | tail -n 20"

step "Verify final topology on all owners"
assert_topology "${HOST01}" "${OWNER0_PORT}" "${CUTOVER_EPOCH}" "0,1,2" "0,1,2" "3" "/tmp/v16_owner0_topology_final.out"
assert_topology "${HOST01}" "${OWNER1_PORT}" "${CUTOVER_EPOCH}" "0,1,2" "0,1,2" "3" "/tmp/v16_owner1_topology_final.out"
assert_topology "${HOST2}" "${OWNER2_PORT}" "${CUTOVER_EPOCH}" "0,1,2" "0,1,2" "3" "/tmp/v16_owner2_topology_final.out"
assert_source_migrated "${HOST01}" "0" "${OWNER0_LOG}"
assert_source_migrated "${HOST01}" "1" "${OWNER1_LOG}"

if [[ "${VERIFY_MIGRATED_DATA}" == "1" ]]; then
    step "Verify migrated prefill data on owner2"
    ssh_run "${HOST01}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_read_verify --host ${HOST2} --port ${OWNER2_PORT} --dim ${DIM} --prefill ${PREFILL_KEYS} --sample-count ${VERIFY_SAMPLE_COUNT} --owners 0,1,2 --target-owner 2 --timeout-ms 10000"
fi

step "Post-cutover write validation"
ssh_run "${HOST01}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${HOST01}:${OWNER0_PORT},${HOST01}:${OWNER1_PORT},${HOST2}:${OWNER2_PORT} --dim ${DIM} --prefill 0 --keyspace ${POST_KEYSPACE} --ops ${POST_OPS} --threads ${POST_THREADS} --pipeline 1 --mode vadd --timeout-ms 10000 >${POST_WRITE_OUT} 2>&1 && cat ${POST_WRITE_OUT}"
ssh_run "${HOST01}" "awk '/^\\[stats node=2\\]/{in_node=1;next} in_node && /^\\[stats\\] total=/{for(i=1;i<=NF;i++) if(\$i ~ /^vadd=/){sub(/^vadd=/,\"\",\$i); if(\$i+0>0) exit 0; else exit 1}} END{if(!in_node) exit 1}' ${POST_WRITE_OUT}"

if [[ "${RUN_POST_READ}" == "1" ]]; then
    step "Post-cutover read validation"
    ssh_run "${HOST01}" "cd ${REMOTE_DIR} && ./benchmark/vemb_v16_bench --transport tcp --endpoints ${HOST01}:${OWNER0_PORT},${HOST01}:${OWNER1_PORT},${HOST2}:${OWNER2_PORT} --dim ${DIM} --prefill 0 --keyspace ${POST_KEYSPACE} --ops ${POST_OPS} --threads ${POST_THREADS} --pipeline 1 --mode vemb-inline --timeout-ms 10000 >${POST_READ_OUT} 2>&1 && cat ${POST_READ_OUT}"
fi

step "Done"
echo "3-owner 2-host scaleout flow finished. Useful logs:"
echo "  owner0: ssh ${SSH_USER}@${HOST01} 'tail -200 ${OWNER0_LOG}'"
echo "  owner1: ssh ${SSH_USER}@${HOST01} 'tail -200 ${OWNER1_LOG}'"
echo "  owner2: ssh ${SSH_USER}@${HOST2} 'tail -200 ${OWNER2_LOG}'"
echo "  coordinator: ssh ${SSH_USER}@${HOST01} 'cat ${COORD_OUT}'"
