#!/usr/bin/env python3
"""kvc_trace_replay.py — FAST25 trace 回放（Mooncake+KVC 栈, 项7）

语义对齐 Mooncake/benchmarks/storage_benchmark_v1:
  - trace jsonl: {timestamp, input_length, output_length, hash_ids[]}
  - 每个 hash_id = 一个 KV 页
  - 首次出现 → put（模拟 prefill 写入）；再次出现 → get（前缀命中读）

模式:
  --loops N: 第 1 遍为首触写（预热, cold-start），后续遍为纯命中读（稳态,
             模拟 KV cache 池已就绪后的多轮前缀复用）。

用法（环境变量同 store_kv_bench 的 KVC 族）:
  python3 kvc_trace_replay.py --trace <trace.jsonl> \
      --max-requests 2000 --page-size 4096 --batch-mode --loops 5
"""
import argparse
import json
import sys
import time


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trace", required=True)
    ap.add_argument("--max-requests", type=int, default=500)
    ap.add_argument("--page-size", type=int, default=4096)
    ap.add_argument("--local-hostname", default="127.0.0.1:50071")
    ap.add_argument("--metadata-server", default="http://127.0.0.1:8080/metadata")
    ap.add_argument("--master-server", default="127.0.0.1:50051")
    ap.add_argument("--key-prefix", default="tr")
    ap.add_argument("--batch-mode", action="store_true",
                    help="按请求分组批量 put/get（对齐真实 connector 行为）")
    ap.add_argument("--loops", type=int, default=1,
                    help="回放遍数: 第 1 遍为首触写(预热), 后续遍为纯命中读(稳态)")
    args = ap.parse_args()

    from mooncake.store import MooncakeDistributedStore
    store = MooncakeDistributedStore()
    rc = store.setup(args.local_hostname, args.metadata_server, 0,
                     32 * 1024 * 1024, "tcp", "", args.master_server)
    assert rc == 0, f"setup failed: {rc}"

    reqs = []
    with open(args.trace) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            reqs.append(r.get("hash_ids", []))
            if len(reqs) >= args.max_requests:
                break
    print(f"trace={args.trace} requests={len(reqs)} loops={args.loops}")

    page = bytes([0x5E]) * args.page_size
    written = set()

    def run_pass(tag):
        st = {"puts": 0, "gets": 0, "put_err": 0, "get_err": 0, "bytes": 0}
        t0 = time.perf_counter()
        for r in reqs:
            new_keys, hit_keys = [], []
            for h in r:
                (hit_keys if h in written else new_keys).append(
                    f"{args.key_prefix}{h:016d}")
            if new_keys:
                if args.batch_mode:
                    rc = store.put_batch(new_keys, [page] * len(new_keys))
                    if rc != 0:
                        st["put_err"] += len(new_keys)
                    else:
                        st["puts"] += len(new_keys)
                        st["bytes"] += len(new_keys) * args.page_size
                        written.update(int(k[len(args.key_prefix):])
                                       for k in new_keys)
                else:
                    for key in new_keys:
                        rc = store.put(key, page)
                        if rc != 0:
                            st["put_err"] += 1
                        else:
                            st["puts"] += 1
                            st["bytes"] += args.page_size
                            written.add(int(key[len(args.key_prefix):]))
            if hit_keys:
                if args.batch_mode:
                    datas = store.get_batch(hit_keys)
                    for d in datas:
                        if d is None or len(d) == 0:
                            st["get_err"] += 1
                        else:
                            st["gets"] += 1
                            st["bytes"] += len(d)
                else:
                    for key in hit_keys:
                        d = store.get(key)
                        if d is None or len(d) == 0:
                            st["get_err"] += 1
                        else:
                            st["gets"] += 1
                            st["bytes"] += len(d)
        dt = time.perf_counter() - t0
        total = st["puts"] + st["gets"]
        hr = st["gets"] / total * 100 if total else 0
        print(f"[{tag}] pages: first_touch={st['puts']} hits={st['gets']} "
              f"(hit_rate={hr:.1f}%)")
        print(f"[{tag}] errors: put={st['put_err']} get={st['get_err']}")
        print(f"[{tag}] wall={dt:.2f}s  ops/s={total/dt:.0f}  "
              f"MiB/s={st['bytes']/dt/1048576:.1f}")
        return st["put_err"] + st["get_err"]

    rc_total = 0
    for loop in range(1, args.loops + 1):
        tag = "warmup" if loop == 1 else f"steady{loop}"
        rc_total += run_pass(tag)
    return 1 if rc_total else 0


if __name__ == "__main__":
    sys.exit(main())
