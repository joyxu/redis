#!/usr/bin/env bash
# Compare local code files with a peer and overwrite only files that differ.
set -euo pipefail

NODE="${NODE:-192.168.90.112}"
SSH_USER="${SSH_USER:-root}"
REMOTE_ROOT="${REMOTE_ROOT:-/root/szz/codespace/hpc-redis}"
SSH_PORT="${SSH_PORT:-22}"
ALL_CODE=0
DRY_RUN=0

case "$NODE" in
    *@*) PEER="$NODE" ;;
    *) PEER="$SSH_USER@$NODE" ;;
esac

usage() {
    cat <<'USAGE'
Usage: [NODE=IP] bash scripts/sync_changed_code_to_peer.sh [options]

Compare local code files with a peer using SHA-256, then overwrite only files
whose content differs. No remote backup is created.

Options:
  --all-code  Compare every tracked/untracked code file, not only local changes.
  --dry-run   Print files that would be overwritten without copying them.
  -h, --help  Show this help.

Defaults:
  NODE        192.168.90.112
  SSH_USER    root
  REMOTE_ROOT /root/szz/codespace/hpc-redis
  SSH_PORT    22
USAGE
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --all-code) ALL_CODE=1 ;;
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
trap 'rm -f "$candidate_file"' EXIT

if [ "$ALL_CODE" -eq 1 ]; then
    git ls-files -co --exclude-standard >"$candidate_file"
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
    remote_hash="$(ssh -q -p "$SSH_PORT" "$PEER" "sha256sum '$remote_path' 2>/dev/null | awk '{print \$1}'" </dev/null)"

    if [ "$local_hash" = "$remote_hash" ]; then
        printf 'same    %s\n' "$rel_path"
        continue
    fi

    different_count=$((different_count + 1))
    printf 'changed %s\n' "$rel_path"
    if [ "$DRY_RUN" -eq 1 ]; then
        continue
    fi

    remote_parent="$REMOTE_ROOT/$(dirname "$rel_path")"
    ssh -q -p "$SSH_PORT" "$PEER" "mkdir -p '$remote_parent'" </dev/null
    printf 'copy    scp -q -P %q %q %q\n' \
        "$SSH_PORT" "$rel_path" "$PEER:$remote_path"
    scp -q -P "$SSH_PORT" "$rel_path" "$PEER:$remote_path" </dev/null
    copied_hash="$(ssh -q -p "$SSH_PORT" "$PEER" "sha256sum '$remote_path' | awk '{print \$1}'" </dev/null)"
    if [ "$copied_hash" != "$local_hash" ]; then
        echo "ERROR: hash mismatch after copy: $rel_path" >&2
        exit 1
    fi
    synced_count=$((synced_count + 1))
done <"$candidate_file"

printf 'candidates=%d different=%d synced=%d mode=%s\n' \
    "$candidate_count" "$different_count" "$synced_count" \
    "$([ "$DRY_RUN" -eq 1 ] && printf dry-run || printf overwrite)"
