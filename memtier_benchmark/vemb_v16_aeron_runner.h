/*
 * Copyright (C) 2026 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef VEMB_V16_AERON_RUNNER_H
#define VEMB_V16_AERON_RUNNER_H

#include "memtier_benchmark.h"
#include "run_stats.h"
#include "obj_gen.h"

/*
 * Side-channel Aeron (TCP control + UB-backed SPSC ring) runner.
 *
 * Bypasses memtier's libevent stack and runs the same wire protocol as
 * benchmark/vemb_v16_bench --transport aeron, but reuses memtier's
 * run_stats / HDR histogram / Totals output.
 *
 * Thread/channel mapping:
 *   -t N          -> N worker pthreads
 *   -c M          -> M channels per worker (total N*M channels)
 *   --pipeline P  -> per-channel outstanding-req cap
 *
 * Mode is derived from cfg (ratio/vsim/vrem flags), mirroring the memtier
 * TCP path's protocol.cpp dispatch. Returns a populated run_stats; caller
 * (run_benchmark) proceeds to its normal Totals/JSON/HDR output.
 *
 * Returns: populated run_stats on success, exit(1) on unrecoverable setup
 * failure (TCP control missing, channel alloc rejected, etc.). Per-op errors are
 * recorded inside the stats, not fatal.
 */
run_stats vemb_v16_aeron_run(benchmark_config* cfg, object_generator* obj_gen);

/* Called by main() after arg parse. Default mode = "aeron" (TCP+UB).
 * For cross-node mode, endpoint = "host:port". */
void vemb_v16_aeron_set_transport(const std::string &mode,
                                  const std::string &endpoint);

#endif /* VEMB_V16_AERON_RUNNER_H */
