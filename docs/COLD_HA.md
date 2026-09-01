```
请求路径（primary）
CLI -> Proxy -> SuperNode/TLC -> Node primary 写入协调逻辑
                                      |
                                      +-> WARM staging（暂不可见）
                                      +-> cold_backup_event payload
                                      |
                                      v
                                  COLD ingress
                                  校验 event
                                      |
                                      v
                         per-node AOF append queue
                              （多生产者入队）
                                      |
                                      v
                              AOF segment writer
                         （单消费者分配 seq、append）
                                      |
                                      v
             segment store / local AOF
                    |
          +---------+-------------------------+
          |                                   |
          v                                   v
 appended seq/progress cursor          append accepted
 （旁路元数据：记录 seq/offset）       （以下两条支路并行）
                                          |
                    +---------------------+---------------------+
                    |                                           |
                    v                                           v
        本地 durable 路径                       异步复制路径
               |                                  |
               v                                  v
  local flush coordinator                 replication cursor
   （group commit 批量策略）                       |
               |                                  v
               v                           replication queue
          durable_seq                             |
               |                                  v
               v                         网络发送到 Follower
  发布 WARM metadata/HA state                     |
               |                                  |
               v                                  v
  CLI LOCAL_DURABLE_ACK                     Follower COLD ingress
                                                  |
                                                  v
                                         Follower AOF append queue
                                                  |
                                                  v
                                         Follower AOF segment writer
                                                  |
                                                  v
                                         Follower local flush coordinator
                                                  |
                                                  v
                                  Follower durable ACK -> replicated_seq
                                                  |
                                                  v
                                          ha_safe_point_seq

后台 checkpoint / compact 路径
HA/WARM state + durable_seq -> checkpoint builder
                                      |
                                      v
                         checkpoint.<generation>.tmp
                                      |
                                      v
                             fsync + atomic rename
                                      |
                                      v
                            manifest / checkpoint catalog
                                      |
                                      v
                        retention / AOF compaction
```