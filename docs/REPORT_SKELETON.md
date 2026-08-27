# 全量数据 20260823 dev_test_0816_retest

> **最优行标记**: 配置 sweep 表 (t1/t2/t3/t4/t8) 中 **加粗行** 为该组最优档——
> 规则: ops_sec 最大者为基准, 若存在吞吐 ≥97% 且 p99 显著更低 (<80%) 的档位则取后者;
> baseline 与 hpc 分组各标一行。t5(阶段)/t6(场景) 行间非同类配置, 不标。

## 3.2.1 单实例性能

### 本地回环 VEMB
#### baseline
> 文件: (e.g. `benchmark/results/regression/20260823_dev_test_0816_retest/t1_vemb_loopback.tsv`)

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


#### hpc TCP
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc Aeron
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


#### 汇总：baseline vs hpc TCP 

| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|
| 1 | 1 | 1 | 56503 | 30453 | **0.54x** | （示例行，仅供展示格式，每个T,C,P 组合都要有对应1行）

#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|

### 本地回环 VSIM_2KEY
#### baseline
> 文件:

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
> 文件:

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc aeron
> 文件:

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|
| 1 | 1 | 1 | 56503 | 30453 | **0.54x** | （示例行，仅供展示格式，每个T,C,P 组合都要有对应1行）

#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|


### 本地回环 VADD
#### baseline
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


#### hpc aeron
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|
| 1 | 1 | 1 | 56503 | 30453 | **0.54x** | （示例行，仅供展示格式，每个T,C,P 组合都要有对应1行）

#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|

### 本地回环 VREM
#### baseline
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc aeron
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|
| 1 | 1 | 1 | 56503 | 30453 | **0.54x** | （示例行，仅供展示格式，每个T,C,P 组合都要有对应1行）

#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|

### 本地回环 64x1x32 高维对比 （DIM=1024，2048，3072）
#### baseline
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

#### 汇总：baseline vs hpc aeron
| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

### 跨节点 VEMB
#### baseline
> 文件:

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


#### hpc TCP
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc Aeron
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


#### 汇总：baseline vs hpc TCP 

| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|


#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|

### 跨节点 VSIM_2KEY
#### baseline
> 文件:

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


#### hpc TCP
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc Aeron
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


#### 汇总：baseline vs hpc TCP 

| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|


#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|

### 跨节点 64x1x32 高维对比 （DIM=1024，2048，3072）
#### baseline
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc Aeron
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

#### 汇总：baseline vs hpc aeron
| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

## 3.2.2 同核数对比
### 本地回环 DIM=300
#### hpc TCP 17:17
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc aeron 17:17
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### baseline 12实例 本地 DIM=300
> 文件:

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|


#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|

### 跨节点 DIM=300
#### hpc TCP 17:17
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc aeron 17:17
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### baseline 12实例
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|


#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|

### 跨节点 DIM=8

#### hpc TCP 17:17
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb | nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc aeron 17:17
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb | nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### baseline 12实例
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb | nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|


#### 汇总：baseline vs hpc aeron
| T | C | P | 原生 ops/core | hpc ops/core | 加速比 |
|---|---|---:|---:|---:|---:|

### 本地回环 64x1x32 高维对比 （DIM=1024，2048，3072）
#### baseline
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc Aeron
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

#### 汇总：baseline vs hpc aeron
| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

### 跨节点 64x1x32 高维对比 （DIM=1024，2048，3072）
#### baseline
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb | nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb | nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc Aeron
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb | nic_util_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

#### 汇总：baseline vs hpc aeron
| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

## 3.2.3 Redis 集群场景对比
### DIM=300 VEMB
#### 原生 redis cluster 4节点
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |


#### Hpc TCP 双机 cluster
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### Hpc Aeron 双机 cluster
> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 集群三方案对比: ops/sec/core

| t | c | pipeline | 原生 ops/sec/core | TCP cluster ops/sec/core | TCP/原生 | Aeron ops/sec/core | Aeron/原生 |
|---|---|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1 | 62914 | 37058 | 0.59x | 49181 | 0.78x | (示例行，仅展示格式)

### 64x1x32 高维对比（DIM=1024，2048，3072）
#### baseline
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc TCP
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### hpc Aeron
| DIM | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

#### 汇总：baseline vs hpc aeron
| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

## 3.2.4 扩容场景下 VEMB 性能

### hpc TCP 扩容

> 文件:

| phase | ops_sec | hits | p50_ms | p99_ms | wall_s | note | core_ut | core_st | si | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 9124521.90 | 9124521.90 | 0.87900 | 1.77500 | 30.491080 | active={0} | 8.85 | 11.83 | 14.29 | 572368 |
| during_scaleout | 9229656.88 | 0.87100 | 1.63900 | NA | 2.150885 | active={0}->{0,1}, memtier-client-topology-retry, endpoints=192.168.1.111:6391,192.168.1.112:6391 | 346.22 | 253.16 | 166.76 | 583028 |
| scaleout_after | 9300165.26 | 9300165.26 | 1.59100 | 3.61500 | 32.993974 | active={0,1}, memtier-client-topology, steady_keys=10001-20000, endpoints=192.168.1.111:6391,192.168.1.112:6391 | 12.18 | 10.59 | 7.42 | 667648 |

### hpc Aeron 扩容

> 文件:
| phase | ops_sec | hits | p50_ms | p99_ms | wall_s | note | core_ut | core_st | si | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 9124521.90 | 9124521.90 | 0.87900 | 1.77500 | 30.491080 | active={0} | 8.85 | 11.83 | 14.29 | 572368 |
| during_scaleout | 9229656.88 | 0.87100 | 1.63900 | NA | 2.150885 | active={0}->{0,1}, memtier-client-topology-retry, endpoints=192.168.1.111:6391,192.168.1.112:6391 | 346.22 | 253.16 | 166.76 | 583028 |
| scaleout_after | 9300165.26 | 9300165.26 | 1.59100 | 3.61500 | 32.993974 | active={0,1}, memtier-client-topology, steady_keys=10001-20000, endpoints=192.168.1.111:6391,192.168.1.112:6391 | 12.18 | 10.59 | 7.42 | 667648 |

### 原生 redis cluster 扩容

> 文件: 

| phase | master_count | duration_s | ops_sec | avg_lat_ms | p50_ms | p99_ms | p999_ms | kb_sec | cores_used | core_ut | core_st | si | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 3 | 60 | 2137279.10 | 3.08888 | 1.94300 | 7.39100 | 202.75100 | 1742540.21 | 8.68 | 4.80 | 3.88 | 3.79 | 290616 |
| during_scaleout | 3->4 | 120 | 2211116.90 | 3.01129 | 1.89500 | 5.82300 | 203.77500 | 1802740.14 | 9.36 | 5.09 | 4.26 | 4.37 | 327548 |
| after | 4 | 60 | 1772915.64 | 3.36700 | 1.76700 | 10.11100 | 208.89500 | 1445373.93 | 8.11 | 4.24 | 3.88 | 3.27 | 310292 |


## 3.2.5 混合读与写/删除

### 混合读+写/删

> 文件: 

| scene | ops_sec | p50_ms | p99_ms | cpu_cores | core_ut | core_st | si | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| read_write | 15159801.88 | 12 | 13 | 30.67 | 19.14 | 11.52 | 21.12 | 312364 |
| read_delete | 3771782.83 | 12 | 13 | 37.61 | 20.61 | 17.00 | 2.39 | 951040 |


## 3.2.7 VSIM_2KEY 双节点性能对比

### HPC TCP

> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

### HPC Aeron

> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

### Baseline

> 文件: 

| op | t | c | pipeline | ops_sec | avg_lat_ms | p50_ms | p99_ms | cores | core_ut | core_st | si | hi | rss_kb |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 汇总：baseline vs hpc TCP 

| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

#### 汇总：baseline vs hpc aeron
| DIM| 原生 ops/core | hpc ops/core | 加速比 |
|---:|---:|---:|---:|

## 3.2.6 本地远端UB 7:1 拓扑读吞吐对比（Aeron 与 TCP）

| 场景 | transport | ops/sec | miss | p50/p99 ms |
| --- | --- | --- | --- | --- |
| pure_local_100k | tcp | 11.02M | 0.86% | 0.69500 / 0.91100 |
| pure_local_100k | aeron | 32.60M | 0.85% | 0.17500 / 0.33500 |
| small_7to1_100k | tcp | 11.50M | 0.00% | 0.71100 / 0.94300 |
| small_7to1_100k | aeron | 32.87M | 0.00% | 0.18300 / 0.32700 |