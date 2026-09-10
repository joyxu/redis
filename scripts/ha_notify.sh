#!/usr/bin/env bash
# keepalived notify hook. Arguments are MASTER, BACKUP, FAULT, status, or retry.
set -u

REDIS_CLI="${HPC_REDIS_HA_REDIS_CLI:-redis-cli}"
REDIS_HOST="${HPC_REDIS_HA_REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${HPC_REDIS_HA_REDIS_PORT:-6379}"
TIMEOUT="${HPC_REDIS_HA_REDIS_TIMEOUT:-5}"
LOG="${HPC_REDIS_HA_LOG:-/var/log/hpc-redis-ha.log}"
STATUS_FILE="${HPC_REDIS_HA_NOTIFY_STATUS:-/run/hpc-redis-keepalived/notify.status}"

if [[ "${REDIS_CLI}" == */* && ! -x "${REDIS_CLI}" ]]; then
    exit 1
fi

log() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >>"${LOG}"
}

write_status() {
    local state=$1 operation=$2 result=$3 rc=$4 attempt=$5 detail=$6
    local status_dir status_tmp
    status_dir="$(dirname "${STATUS_FILE}")"
    mkdir -p "${status_dir}" 2>/dev/null || return 1
    status_tmp="$(mktemp "${STATUS_FILE}.tmp.XXXXXX")" || return 1
    detail="$(printf '%s' "${detail}" | tr '\n' ' ' | tr '\r' ' ')"
    {
        printf 'state=%s\n' "${state}"
        printf 'operation=%s\n' "${operation}"
        printf 'result=%s\n' "${result}"
        printf 'rc=%s\n' "${rc}"
        printf 'attempt=%s\n' "${attempt}"
        printf 'timestamp=%s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')"
        printf 'detail=%s\n' "${detail}"
    } >"${status_tmp}" && mv -f "${status_tmp}" "${STATUS_FILE}"
    rm -f "${status_tmp}"
}

show_status() {
    if [ -r "${STATUS_FILE}" ]; then
        cat "${STATUS_FILE}"
        return 0
    fi
    printf 'state=UNKNOWN\nresult=unknown\nstatus_file=%s\n' "${STATUS_FILE}"
    return 1
}

last_failed_state() {
    [ -r "${STATUS_FILE}" ] || return 1
    awk -F= '$1 == "result" { result=$2 } $1 == "state" { state=$2 } END {
        if (result == "failed" && (state == "MASTER" || state == "BACKUP" || state == "FAULT"))
            print state
    }' "${STATUS_FILE}"
}

if [ "${1:-}" = status ]; then
    show_status
    exit $?
fi
if [ "${1:-}" = retry ]; then
    retry_state="$(last_failed_state || true)"
    case "${retry_state}" in
        MASTER|BACKUP|FAULT) exec "$0" "${retry_state}" ;;
        *) log "notify retry skipped: no failed state"; exit 1 ;;
    esac
fi

command -v timeout >/dev/null 2>&1 || {
    log "notify unavailable: timeout command not found"
    exit 1
}

case "${1:-}" in
MASTER) command=promote; operation=PROMOTE ;;
BACKUP) command=demote; operation=DEMOTE ;;
FAULT) command=fence; operation=FENCE ;;
*) log "unknown keepalived state: ${1:-<none>}"; exit 2 ;;
esac

attempt=1
if [ -r "${STATUS_FILE}" ]; then
    previous_attempt="$(awk -F= '$1 == "attempt" { print $2 }' "${STATUS_FILE}")"
    case "${previous_attempt}" in
        ''|*[!0-9]*) ;;
        *) attempt=$((previous_attempt + 1)) ;;
    esac
fi
result="$(timeout --signal=TERM "${TIMEOUT}s" "${REDIS_CLI}" \
    -h "${REDIS_HOST}" -p "${REDIS_PORT}" HA "${command}" 2>&1)"
rc=$?
effective_rc=${rc}
if [ "${effective_rc}" -eq 0 ] &&
   printf '%s' "${result}" | grep -Eqi '^ERR([[:space:]]|$)'; then
    # redis-cli may exit 0 even when the server returned a RESP error.
    effective_rc=1
fi
if [ "${effective_rc}" -ne 0 ] && [ "${command}" = promote ] &&
   printf '%s' "${result}" | grep -Eqi 'already leader'; then
    effective_rc=0
    result="${result} (idempotent)"
fi
if [ "${effective_rc}" -eq 0 ]; then
    write_status "${1}" "${operation}" success "${effective_rc}" "${attempt}" "${result}" || true
else
    write_status "${1}" "${operation}" failed "${effective_rc}" "${attempt}" "${result}" || true
fi
log "keepalived -> ${1}: HA.${operation} rc=${rc} effective_rc=${effective_rc} attempt=${attempt}: ${result}"
exit "${effective_rc}"
