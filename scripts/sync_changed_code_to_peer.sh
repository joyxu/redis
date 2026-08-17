#!/usr/bin/env bash
# Compare local code files with a peer and overwrite only files that differ.
set -euo pipefail

NODE="${NODE:-43.154.145.18}"
SSH_USER="${SSH_USER:-root}"
REMOTE_ROOT="${REMOTE_ROOT:-/root/szz/codespace/hpc-redis}"
SSH_PORT="${SSH_PORT:-8112}"
ALL_CODE=0
DRY_RUN=0
BUILD_TARGET=""
VERIFY_BUILD_TARGET=""
VERBOSE=0

case "$NODE" in
    *@*) PEER="$NODE" ;;
    *) PEER="$SSH_USER@$NODE" ;;
esac

CONTROL_PATH="${TMPDIR:-/tmp}/vemb_sync_${USER:-user}_$$_${RANDOM}"
SSH_OPTIONS=(-q -p "$SSH_PORT" -o ControlMaster=auto -o ControlPersist=60 \
    -o "ControlPath=$CONTROL_PATH")
SCP_OPTIONS=(-q -P "$SSH_PORT" -o "ControlPath=$CONTROL_PATH")

usage() {
    cat <<'USAGE'
Usage: [NODE=HOST] bash scripts/sync_changed_code_to_peer.sh [options]

Compare local code files with a peer using SHA-256, then overwrite only files
whose content differs. No remote backup is created.

Options:
  --all-code  Compare every tracked/untracked code file, not only local changes.
  --build TARGET  Force-rebuild TARGET after sync: server, client, or all.
  --verify-build TARGET  Verify TARGET's source/binary build stamp after sync.
  --verbose   Print files whose content already matches the peer.
  --dry-run   Print files that would be overwritten without copying them.
  -h, --help  Show this help.

Defaults:
  NODE        43.154.145.18 (112 external endpoint)
  SSH_USER    root
  REMOTE_ROOT /root/szz/codespace/hpc-redis
  SSH_PORT    8112
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --all-code) ALL_CODE=1 ;;
        --build)
            shift
            [ "$#" -gt 0 ] || { echo "ERROR: --build requires a target" >&2; exit 2; }
            BUILD_TARGET="$1"
            ;;
        --verify-build)
            shift
            [ "$#" -gt 0 ] || { echo "ERROR: --verify-build requires a target" >&2; exit 2; }
            VERIFY_BUILD_TARGET="$1"
            ;;
        --verbose) VERBOSE=1 ;;
        --dry-run) DRY_RUN=1 ;;
        -h|--help) usage; exit 0 ;;
        -*) echo "ERROR: unknown option: $1" >&2; usage >&2; exit 2 ;;
        *) echo "ERROR: unexpected argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

is_code_file() {
    case "$1" in
        *.c|*.cc|*.cpp|*.cxx|*.h|*.hh|*.hpp|*.inc|*.S|*.s|*.go|*.rs|*.java|*.py|*.pl|*.rb|*.sh|*.mk|Makefile|*/Makefile|GNUmakefile|*/GNUmakefile|CMakeLists.txt|*/CMakeLists.txt|meson.build|*/meson.build)
            return 0
            ;;
        *) return 1 ;;
    esac
}

local_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

candidate_file="$(mktemp)"
cleanup() {
    ssh "${SSH_OPTIONS[@]}" -O exit "$PEER" >/dev/null 2>&1 || true
    rm -f "$candidate_file"
}
trap cleanup EXIT

if [ "$ALL_CODE" -eq 1 ]; then
    {
        git ls-files
        git ls-files -o --exclude-standard
    } | sort -u >"$candidate_file"
else
    {
        git diff --name-only HEAD
        git ls-files -o --exclude-standard
    } | sort -u >"$candidate_file"
fi

candidate_count=0
different_count=0
synced_count=0

while IFS= read -r rel_path; do
    [ -n "$rel_path" ] || continue
    is_code_file "$rel_path" || continue
    [ -f "$rel_path" ] || continue

    candidate_count=$((candidate_count + 1))
    local_hash="$(local_sha256 "$rel_path")"
    remote_path="$REMOTE_ROOT/$rel_path"
    if ! remote_hash="$(ssh "${SSH_OPTIONS[@]}" "$PEER" \
        "sha256sum '$remote_path' 2>/dev/null | awk '{print \$1}'" </dev/null)"; then
        echo "ERROR: remote hash check failed: $rel_path" >&2
        exit 1
    fi

    if [ "$local_hash" = "$remote_hash" ]; then
        [ "$VERBOSE" -eq 1 ] && printf 'same    %s\n' "$rel_path"
        continue
    fi

    different_count=$((different_count + 1))
    printf 'changed %s\n' "$rel_path"
    if [ "$DRY_RUN" -eq 1 ]; then
        continue
    fi

    remote_parent="$REMOTE_ROOT/$(dirname "$rel_path")"
    if ! ssh "${SSH_OPTIONS[@]}" "$PEER" "mkdir -p '$remote_parent'" </dev/null; then
        echo "ERROR: remote directory creation failed: $rel_path" >&2
        exit 1
    fi
    printf 'copy    scp -q -P %q %q %q\n' \
        "$SSH_PORT" "$rel_path" "$PEER:$remote_path"
    if ! scp "${SCP_OPTIONS[@]}" "$rel_path" "$PEER:$remote_path" </dev/null; then
        echo "ERROR: remote copy failed: $rel_path" >&2
        exit 1
    fi
    if ! copied_hash="$(ssh "${SSH_OPTIONS[@]}" "$PEER" \
        "sha256sum '$remote_path' | awk '{print \$1}'" </dev/null)"; then
        echo "ERROR: remote post-copy hash check failed: $rel_path" >&2
        exit 1
    fi
    if [ "$copied_hash" != "$local_hash" ]; then
        echo "ERROR: hash mismatch after copy: $rel_path" >&2
        exit 1
    fi
    synced_count=$((synced_count + 1))
done <"$candidate_file"

valid_build_target() {
    case "$1" in server|client|all) return 0 ;; *) return 1 ;; esac
}

if [ -n "$BUILD_TARGET" ] && ! valid_build_target "$BUILD_TARGET"; then
    echo "ERROR: invalid --build target: $BUILD_TARGET" >&2
    exit 2
fi
if [ -n "$VERIFY_BUILD_TARGET" ] && ! valid_build_target "$VERIFY_BUILD_TARGET"; then
    echo "ERROR: invalid --verify-build target: $VERIFY_BUILD_TARGET" >&2
    exit 2
fi

if [ "$DRY_RUN" -eq 0 ] && [ -n "$BUILD_TARGET" ]; then
    ssh "${SSH_OPTIONS[@]}" "$PEER" \
        "cd '$REMOTE_ROOT' && bash scripts/vemb_v16_build_stamp.sh build '$BUILD_TARGET'" \
        </dev/null
fi

if [ "$DRY_RUN" -eq 0 ] && [ -n "$VERIFY_BUILD_TARGET" ]; then
    ssh "${SSH_OPTIONS[@]}" "$PEER" \
        "cd '$REMOTE_ROOT' && bash scripts/vemb_v16_build_stamp.sh verify '$VERIFY_BUILD_TARGET'" \
        </dev/null
fi

printf 'candidates=%d different=%d synced=%d mode=%s\n' \
    "$candidate_count" "$different_count" "$synced_count" \
    "$([ "$DRY_RUN" -eq 1 ] && printf dry-run || printf overwrite)"

if [ "$DRY_RUN" -eq 0 ] && [ "$synced_count" -gt 0 ] && [ -z "$BUILD_TARGET" ]; then
    echo "NOTE: code was synced without rebuilding; use --build server or --build client before testing." >&2
fi
