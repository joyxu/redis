#!/usr/bin/env bash
# Build, install, configure, and control keepalived 1.2.2 for HPC-Redis HA.
set -euo pipefail

REMOTE_HOST="${REMOTE_HOST:-43.154.145.18}"
REMOTE_USER="${REMOTE_USER:-root}"
REMOTE_PORT="${REMOTE_PORT:-8111}"
NODE_ID="${NODE_ID:-111}"
LVS_KEEPALIVED_ROOT="${LVS_KEEPALIVED_ROOT:-/root/szz/codespace/LVS/tools/keepalived}"
INSTALL_ROOT="${KEEPALIVED_INSTALL_ROOT:-/opt/hpc-redis/keepalived}"
CONFIG_PATH="${KEEPALIVED_CONFIG:-/etc/keepalived/keepalived.conf}"
PID_DIR="${KEEPALIVED_PID_DIR:-/run/hpc-redis-keepalived}"
VIP="${HPC_REDIS_HA_VIP:-}"
INTERFACE="${HPC_REDIS_HA_INTERFACE:-}"
PRIORITY="${HPC_REDIS_HA_PRIORITY:-}"
VRID="${HPC_REDIS_HA_VRID:-60}"
AUTH_PASS="${HPC_REDIS_HA_AUTH_PASS:-hpcredis}"
CHECK_INTERVAL="${HPC_REDIS_HA_CHECK_INTERVAL:-2}"
CHECK_FALL="${HPC_REDIS_HA_CHECK_FALL:-3}"
CHECK_RISE="${HPC_REDIS_HA_CHECK_RISE:-2}"
ADVERT_INT="${HPC_REDIS_HA_ADVERT_INT:-1}"
ACTION=deploy
ALL_NODES=0
DRY_RUN=0
INSTALL_DEPS=0

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TEMPLATE="$ROOT/scripts/keepalived_hpc_redis.conf.in"
SSH_CONTROL_PATH="/tmp/hpc_redis_keepalived_${USER:-user}_$$_%p"

usage() {
    cat <<'USAGE'
Usage:
  deploy_keepalived_ha.sh --node 111|112 --vip VIP/CIDR [options] [action]
  deploy_keepalived_ha.sh --all --vip VIP/CIDR [options] [action]

Actions: deploy (default), install, check, start, stop, restart, reload, status.
deploy builds the existing remote LVS/tools/keepalived tree, installs the
binary below /opt/hpc-redis/keepalived, copies HA hooks, writes the config,
and validates it. It leaves keepalived stopped; start is explicit.

Options:
  --vip CIDR       Required for all actions except status, e.g. 192.168.90.202/24
  --node ID        111 or 112 (default: 111)
  --all            Apply the action to both 111 (ssh port 8111) and 112 (8112)
  --host HOST      SSH host (default: 43.154.145.18)
  --ssh-port PORT  SSH port (default: 8111; node 112 defaults to 8112)
  --user USER      SSH user (default: root)
  --interface IF   VRRP interface (111=eth1, 112=eth0)
  --priority N     VRRP priority (111=100, 112=90)
  --vrid N         VRRP virtual_router_id (default: 60)
  --auth-pass P    VRRP PASS value (default: hpcredis)
  --install-deps   Install popt-devel through the remote package manager
  --dry-run        Print operations without changing the remote node
USAGE
}

die() { printf 'ERROR: %s\n' "$*" >&2; exit 2; }

while [ "$#" -gt 0 ]; do
    case "$1" in
        --vip) [ "$#" -ge 2 ] || die "--vip requires CIDR"; VIP="$2"; shift ;;
        --node) [ "$#" -ge 2 ] || die "--node requires 111 or 112"; NODE_ID="$2"; shift ;;
        --all) ALL_NODES=1 ;;
        --host) [ "$#" -ge 2 ] || die "--host requires a value"; REMOTE_HOST="$2"; shift ;;
        --ssh-port) [ "$#" -ge 2 ] || die "--ssh-port requires a value"; REMOTE_PORT="$2"; shift ;;
        --user) [ "$#" -ge 2 ] || die "--user requires a value"; REMOTE_USER="$2"; shift ;;
        --interface) [ "$#" -ge 2 ] || die "--interface requires a value"; INTERFACE="$2"; shift ;;
        --priority) [ "$#" -ge 2 ] || die "--priority requires a value"; PRIORITY="$2"; shift ;;
        --vrid) [ "$#" -ge 2 ] || die "--vrid requires a value"; VRID="$2"; shift ;;
        --auth-pass) [ "$#" -ge 2 ] || die "--auth-pass requires a value"; AUTH_PASS="$2"; shift ;;
        --install-deps) INSTALL_DEPS=1 ;;
        --dry-run) DRY_RUN=1 ;;
        deploy|install|check|start|stop|restart|reload|status) ACTION="$1" ;;
        -h|--help) usage; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
    shift
done

case "$NODE_ID" in
    111) [ -n "$INTERFACE" ] || INTERFACE=eth1; [ -n "$PRIORITY" ] || PRIORITY=100 ;;
    112) [ -n "$INTERFACE" ] || INTERFACE=eth0; [ -n "$PRIORITY" ] || PRIORITY=90; [ "$REMOTE_PORT" = 8111 ] && REMOTE_PORT=8112 ;;
    *) die "--node must be 111 or 112" ;;
esac
[ "$ACTION" = status ] || case "$VIP" in */*) : ;; *) die "--vip CIDR is required" ;; esac
[ "$ACTION" = status ] || {
    vip_addr="${VIP%/*}"
    vip_prefix="${VIP#*/}"
    [[ "$vip_addr" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]] || die "invalid VIP address: $vip_addr"
    [[ "$vip_prefix" =~ ^[0-9]{1,2}$ ]] || die "invalid VIP prefix: $vip_prefix"
    [ "$vip_prefix" -le 32 ] || die "VIP prefix must be between 0 and 32"
    IFS=. read -r vip_a vip_b vip_c vip_d <<<"$vip_addr"
    for vip_octet in "$vip_a" "$vip_b" "$vip_c" "$vip_d"; do
        [ "$((10#$vip_octet))" -le 255 ] || die "VIP octet out of range: $vip_octet"
    done
}
[ "$ALL_NODES" -eq 0 ] || [ "$REMOTE_PORT" = 8111 ] || [ "$REMOTE_PORT" = 8112 ] || die "--all requires ssh port 8111 or 8112"

render_config() {
    local output="$1"
    sed \
        -e "s|@ROUTER_ID@|HPC_REDIS_$NODE_ID|g" \
        -e "s|@INSTALL_ROOT@|$INSTALL_ROOT|g" \
        -e "s|@CHECK_INTERVAL@|$CHECK_INTERVAL|g" \
        -e "s|@CHECK_FALL@|$CHECK_FALL|g" \
        -e "s|@CHECK_RISE@|$CHECK_RISE|g" \
        -e "s|@INTERFACE@|$INTERFACE|g" \
        -e "s|@VRID@|$VRID|g" \
        -e "s|@PRIORITY@|$PRIORITY|g" \
        -e "s|@ADVERT_INT@|$ADVERT_INT|g" \
        -e "s|@AUTH_PASS@|$AUTH_PASS|g" \
        -e "s|@VIP@|$VIP|g" \
        "$TEMPLATE" >"$output"
}

do_action() {
    local peer="$REMOTE_USER@$REMOTE_HOST"
    local tmp_config
    local ssh_opts=(-q -p "$REMOTE_PORT" -o BatchMode=yes -o ConnectTimeout=10
        -o ControlMaster=auto -o ControlPersist=60 -o "ControlPath=$SSH_CONTROL_PATH")
    local scp_opts=(-q -P "$REMOTE_PORT" -o BatchMode=yes -o ConnectTimeout=10
        -o "ControlPath=$SSH_CONTROL_PATH")
    tmp_config="$(mktemp "${TMPDIR:-/tmp}/keepalived_hpc_redis.XXXXXX")"
    render_config "$tmp_config"

    remote() {
        if [ "$DRY_RUN" -eq 1 ]; then
            printf '+ ssh -p %q %q %s\n' "$REMOTE_PORT" "$peer" "$*"
        else
            ssh "${ssh_opts[@]}" "$peer" "$@"
        fi
    }
    copy_to_remote() {
        if [ "$DRY_RUN" -eq 1 ]; then
            printf '+ scp -P %q %q %q\n' "$REMOTE_PORT" "$1" "$peer:$2"
        else
            scp "${scp_opts[@]}" "$1" "$peer:$2"
        fi
    }

    case "$ACTION" in
        deploy|install)
            remote "set -e
                test -d '$LVS_KEEPALIVED_ROOT'
                command -v gcc >/dev/null
                command -v make >/dev/null
                if [ '$INSTALL_DEPS' -eq 1 ]; then dnf install -y popt-devel; fi
                cd '$LVS_KEEPALIVED_ROOT'
                ./configure --prefix='$INSTALL_ROOT' --disable-lvs
                make -j\$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 2)
                make install
                install -d '$INSTALL_ROOT/scripts' '$PID_DIR' /etc/keepalived /var/log
            "
            copy_to_remote "$ROOT/scripts/ha_healthcheck.sh" "$INSTALL_ROOT/scripts/ha_healthcheck.sh"
            copy_to_remote "$ROOT/scripts/ha_notify.sh" "$INSTALL_ROOT/scripts/ha_notify.sh"
            copy_to_remote "$tmp_config" "$CONFIG_PATH"
            remote "set -e
                chmod 0755 '$INSTALL_ROOT/scripts/ha_healthcheck.sh' '$INSTALL_ROOT/scripts/ha_notify.sh'
                chmod 0644 '$CONFIG_PATH'
                set +e
                timeout --signal=TERM --kill-after=1s 3s '$INSTALL_ROOT/sbin/keepalived' -n -l -d -P -f '$CONFIG_PATH' -p '$PID_DIR/check-main.pid' -r '$PID_DIR/check-vrrp.pid' >/tmp/hpc-redis-keepalived-config-check.log 2>&1
                check_rc=\$?
                set -e
                case \$check_rc in 0|124|143) : ;; *) cat /tmp/hpc-redis-keepalived-config-check.log; exit \$check_rc ;; esac
                if grep -Eiq 'unknown keyword|configuration error|cannot open|failed' /tmp/hpc-redis-keepalived-config-check.log; then cat /tmp/hpc-redis-keepalived-config-check.log; exit 1; fi
                rm -f '$PID_DIR/check-main.pid' '$PID_DIR/check-vrrp.pid'
            "
            printf 'node=%s action=%s config=%s keepalived=stopped\n' "$NODE_ID" "$ACTION" "$CONFIG_PATH"
            ;;
        check)
            remote "set -e
                set +e
                timeout --signal=TERM --kill-after=1s 3s '$INSTALL_ROOT/sbin/keepalived' -n -l -d -P -f '$CONFIG_PATH' -p '$PID_DIR/check-main.pid' -r '$PID_DIR/check-vrrp.pid' >/tmp/hpc-redis-keepalived-config-check.log 2>&1
                check_rc=\$?
                set -e
                case \$check_rc in 0|124|143) : ;; *) cat /tmp/hpc-redis-keepalived-config-check.log; exit \$check_rc ;; esac
                if grep -Eiq 'unknown keyword|configuration error|cannot open|failed' /tmp/hpc-redis-keepalived-config-check.log; then cat /tmp/hpc-redis-keepalived-config-check.log; exit 1; fi
                rm -f '$PID_DIR/check-main.pid' '$PID_DIR/check-vrrp.pid'
            "
            printf 'node=%s config=valid\n' "$NODE_ID"
            ;;
        start)
            remote "set -e; test -x '$INSTALL_ROOT/sbin/keepalived'; install -d '$PID_DIR'; '$INSTALL_ROOT/sbin/keepalived' -f '$CONFIG_PATH' -p '$PID_DIR/keepalived.pid' -r '$PID_DIR/vrrp.pid' -c '$PID_DIR/checkers.pid' -D; sleep 1; test -s '$PID_DIR/keepalived.pid'"
            printf 'node=%s keepalived=started\n' "$NODE_ID"
            ;;
        stop)
            remote "if test -s '$PID_DIR/keepalived.pid'; then kill \"\$(cat '$PID_DIR/keepalived.pid')\" 2>/dev/null || true; fi; sleep 1; ip addr del '$VIP' dev '$INTERFACE' 2>/dev/null || true; rm -f '$PID_DIR/keepalived.pid' '$PID_DIR/vrrp.pid' '$PID_DIR/checkers.pid'"
            printf 'node=%s keepalived=stopped\n' "$NODE_ID"
            ;;
        restart)
            remote "if test -s '$PID_DIR/keepalived.pid'; then kill \"\$(cat '$PID_DIR/keepalived.pid')\" 2>/dev/null || true; fi; sleep 1; ip addr del '$VIP' dev '$INTERFACE' 2>/dev/null || true; rm -f '$PID_DIR/keepalived.pid' '$PID_DIR/vrrp.pid' '$PID_DIR/checkers.pid'; '$INSTALL_ROOT/sbin/keepalived' -f '$CONFIG_PATH' -p '$PID_DIR/keepalived.pid' -r '$PID_DIR/vrrp.pid' -c '$PID_DIR/checkers.pid' -D; sleep 1; test -s '$PID_DIR/keepalived.pid'"
            printf 'node=%s keepalived=restarted\n' "$NODE_ID"
            ;;
        reload)
            remote "set -e; test -s '$PID_DIR/keepalived.pid'; kill -HUP \"\$(cat '$PID_DIR/keepalived.pid')\""
            printf 'node=%s keepalived=reloaded\n' "$NODE_ID"
            ;;
        status)
            remote "if test -s '$PID_DIR/keepalived.pid' && kill -0 \"\$(cat '$PID_DIR/keepalived.pid')\" 2>/dev/null; then echo running pid=\$(cat '$PID_DIR/keepalived.pid'); else echo stopped; fi; ip -4 addr show '$INTERFACE' | grep -F '${VIP%/*}' || true"
            ;;
    esac
    rm -f "$tmp_config"
}

if [ "$ALL_NODES" -eq 1 ]; then
    for node_port in 8111:111 8112:112; do
        REMOTE_PORT="${node_port%%:*}"
        NODE_ID="${node_port##*:}"
        if [ "$NODE_ID" = 111 ]; then INTERFACE=eth1; PRIORITY=100; else INTERFACE=eth0; PRIORITY=90; fi
        do_action
    done
else
    do_action
fi
