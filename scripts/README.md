NODE=192.168.90.112 BUILD=1 WORKERS=20:20  REMOTE_DIR=/root/szz/codespace/hpc-redis_bench  bash scripts/run_host_mt_server_flamegraph.sh

NODE=192.168.90.112 bash scripts/sync_changed_code_to_peer.sh --dry-run