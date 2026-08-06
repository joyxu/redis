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
