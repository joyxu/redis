#!/usr/bin/env bash
# Record and verify that VEMB binaries were built from the current sources.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT"

usage() {
    cat <<'USAGE'
Usage: bash scripts/vemb_v16_build_stamp.sh <build|write|verify> <server|client|all>

build   Force-rebuild the selected target with USE_SVE=yes, then write its stamp.
write   Record the current source and binary hashes after a manual build.
verify  Fail unless the source and binary hashes match the recorded stamp.
USAGE
}

# Keep all cross-node data-plane artifacts on the same performance policy.
# OPT is intentional: src/.make-settings may contain a generated OPT value,
# while command-line OPT takes precedence and preserves LTO on the server.
SERVER_OPT='-O3 -flto -fno-omit-frame-pointer'
SDK_CFLAGS='-O3 -flto -g'
SDK_ARCH_FLAGS='-DUSE_ARM_SVE -march=armv8.2-a+sve'
MEMTIER_CFLAGS='-O3 -flto -g -DUSE_SVE -DUSE_ARM_SVE -march=armv8.2-a+sve'
MEMTIER_CXXFLAGS='-O3 -flto -g -Wall -Drestrict=__restrict__ -DUSE_SVE -DUSE_ARM_SVE -march=armv8.2-a+sve'

build_policy() {
    case "$1" in
        server)
            printf '%s\n' 'USE_SVE=yes'
            printf 'OPT=%s\n' "$SERVER_OPT"
            ;;
        client)
            printf '%s\n' 'USE_SVE=yes'
            printf 'SDK_CFLAGS=%s\n' "$SDK_CFLAGS"
            printf 'SDK_ARCH_FLAGS=%s\n' "$SDK_ARCH_FLAGS"
            printf 'MEMTIER_CFLAGS=%s\n' "$MEMTIER_CFLAGS"
            printf 'MEMTIER_CXXFLAGS=%s\n' "$MEMTIER_CXXFLAGS"
            ;;
        *) return 2 ;;
    esac
}

build_policy_sha256() {
    build_policy "$1" | file_sha256 /dev/stdin
}

file_sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

source_sha256() {
    local target=$1
    {
        find src -type f \( -name '*.c' -o -name '*.h' -o -name '*.inc' -o \
            -name '*.S' -o -name '*.s' -o -name '*.mk' -o -name 'Makefile' \)
        if [ "$target" = client ]; then
            find clients/c memtier_benchmark -type f \( -name '*.c' -o -name '*.cc' -o \
                -name '*.cpp' -o -name '*.h' -o -name '*.hpp' -o -name '*.mk' -o \
                -name 'Makefile' \)
        fi
        [ -f src/.make-settings ] && printf '%s\n' src/.make-settings
    } | LC_ALL=C sort -u | while IFS= read -r path; do
        printf '%s %s\n' "$path" "$(file_sha256 "$path")"
    done | file_sha256 /dev/stdin
}

artifact_paths() {
    case "$1" in
        server) printf '%s\n' src/redis-server ;;
        client)
            printf '%s\n' clients/c/build/libvemb_v16_client.a
            printf '%s\n' memtier_benchmark/memtier_benchmark
            ;;
        *) return 2 ;;
    esac
}

stamp_path() {
    printf '%s/.vemb_v16_build_stamp.%s\n' "$ROOT" "$1"
}

write_stamp() {
    local target=$1 stamp tmp path
    stamp="$(stamp_path "$target")"
    tmp="${stamp}.tmp.$$"
    trap 'rm -f "$tmp"' RETURN

    while IFS= read -r path; do
        [ -f "$path" ] || {
            echo "ERROR: missing $target artifact: $path" >&2
            return 1
        }
    done < <(artifact_paths "$target")

    {
        printf 'target=%s\n' "$target"
        printf 'build_policy_sha256=%s\n' "$(build_policy_sha256 "$target")"
        build_policy "$target" | sed 's/^/build_policy=/'
        printf 'source_sha256=%s\n' "$(source_sha256 "$target")"
        while IFS= read -r path; do
            printf 'artifact=%s %s\n' "$path" "$(file_sha256 "$path")"
        done < <(artifact_paths "$target")
        printf 'built_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } >"$tmp"
    mv "$tmp" "$stamp"
    trap - RETURN
    printf 'wrote VEMB %s build stamp: %s\n' "$target" "$stamp"
}

verify_stamp() {
    local target=$1 stamp expected actual expected_policy actual_policy path hash
    stamp="$(stamp_path "$target")"
    [ -f "$stamp" ] || {
        echo "ERROR: missing VEMB $target build stamp; run sync with --build $target" >&2
        return 1
    }

    expected="$(awk -F= '$1 == "source_sha256" { print $2; exit }' "$stamp")"
    actual="$(source_sha256 "$target")"
    [ -n "$expected" ] && [ "$expected" = "$actual" ] || {
        echo "ERROR: VEMB $target sources changed after the recorded build" >&2
        return 1
    }

    expected_policy="$(awk -F= '$1 == "build_policy_sha256" { print $2; exit }' "$stamp")"
    actual_policy="$(build_policy_sha256 "$target")"
    [ -n "$expected_policy" ] && [ "$expected_policy" = "$actual_policy" ] || {
        echo "ERROR: VEMB $target build policy differs from the recorded build" >&2
        return 1
    }

    while IFS=' ' read -r path hash; do
        [ -f "$path" ] && [ "$(file_sha256 "$path")" = "$hash" ] || {
            echo "ERROR: VEMB $target artifact is missing or differs: $path" >&2
            return 1
        }
    done < <(awk -F= '$1 == "artifact" { print $2 }' "$stamp")
    printf 'VEMB %s build stamp: OK\n' "$target"
}

build_server() {
    make -B -C src redis-server USE_SVE=yes OPT="$SERVER_OPT"
    write_stamp server
}

build_client() {
    make -B -C clients/c USE_SVE=yes CFLAGS="$SDK_CFLAGS" ARCH_FLAGS="$SDK_ARCH_FLAGS"
    make -B -C memtier_benchmark USE_SVE=yes CFLAGS="$MEMTIER_CFLAGS" \
        CXXFLAGS="$MEMTIER_CXXFLAGS"
    write_stamp client
}

run_build_locked() {
    local target=$1
    shift
    if ! command -v flock >/dev/null 2>&1; then
        "$@"
        return
    fi
    (
        flock -n 9 || {
            echo "ERROR: VEMB $target build is already running" >&2
            exit 1
        }
        "$@"
    ) 9>"$ROOT/.vemb_v16_build_lock.$target"
}

build_target() {
    case "$1" in
        server) run_build_locked server build_server ;;
        client) run_build_locked client build_client ;;
        all)
            build_target server
            build_target client
            ;;
        *) usage >&2; return 2 ;;
    esac
}

run_for_target() {
    local action=$1 target=$2
    case "$target" in
        server|client) "$action" "$target" ;;
        all)
            "$action" server
            "$action" client
            ;;
        *) usage >&2; return 2 ;;
    esac
}

[ "$#" -eq 2 ] || { usage >&2; exit 2; }
case "$1" in
    build) build_target "$2" ;;
    write) run_for_target write_stamp "$2" ;;
    verify) run_for_target verify_stamp "$2" ;;
    *) usage >&2; exit 2 ;;
esac
