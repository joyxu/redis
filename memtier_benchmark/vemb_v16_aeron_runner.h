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
 * Memtier workload/statistics adapter over the VEMB SDK common core.
 * TCP bootstrap/control plus local or peer-view UB resources are selected by
 * topology and SDK transport resolution; this runner never opens or polls a
 * data-plane channel itself.
 */
run_stats vemb_v16_aeron_run(benchmark_config* cfg, object_generator* obj_gen);

/* Called by main() after arg parse. Default mode = "aeron" (TCP+UB).
 * For cross-node mode, endpoint = "host:port". */
void vemb_v16_aeron_set_transport(const std::string &mode,
                                  const std::string &endpoint,
                                  bool control_uds = false);

#endif /* VEMB_V16_AERON_RUNNER_H */
