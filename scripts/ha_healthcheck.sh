#!/usr/bin/env bash
# keepalived vrrp_script health check for one HPC-Redis instance.
set -u

REDIS_CLI="${HPC_REDIS_HA_REDIS_CLI:-redis-cli}"
REDIS_HOST="${HPC_REDIS_HA_REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${HPC_REDIS_HA_REDIS_PORT:-6379}"
TIMEOUT="${HPC_REDIS_HA_REDIS_TIMEOUT:-2}"

if [[ "${REDIS_CLI}" == */* && ! -x "${REDIS_CLI}" ]]; then
    exit 1
fi
command -v timeout >/dev/null 2>&1 || exit 1

redis_command() {
    timeout --signal=TERM "${TIMEOUT}s" "${REDIS_CLI}" "$@"
}

ping_result="$(redis_command -h "${REDIS_HOST}" -p "${REDIS_PORT}" PING 2>/dev/null || true)"
if [[ "${ping_result}" != "PONG" ]]; then
    exit 1
fi

state="$(redis_command -h "${REDIS_HOST}" -p "${REDIS_PORT}" HA STATE 2>/dev/null || true)"
if [[ -z "${state}" || "${state}" == *FAULT* || "${state}" == *FENCED* ]] || \
   ! grep -Eq '(^|[[:space:]])(MASTER|BACKUP|RECOVERING)([[:space:]]|$)' <<<"${state}"; then
    exit 1
fi
exit 0
