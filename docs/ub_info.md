## ub info
nc 写 --> cc 读: 官方给的数据是 500ns 左右

• 对。每个节点的 `1-4` 都是本节点的本地 CC region，`5-8` 是对端
  `1-4` 的 imported NC view。该模型应固定为：

  - request：112:`/dev/obmm_shmdev7` NC 写入，111:`/dev/obmm_shmdev3`
    本地 CC 读取。
  - response：111:`/dev/obmm_shmdev6` NC 写入，112:`/dev/obmm_shmdev2`
    本地 CC 读取。

  UB 保证 NC 写到对端本地 CC 读的可见性，不应额外引入 dc ivac/cvac 作为数据面同步方案。

  当前代码需要按“资源方向”明确 cacheability，不能以 path 是否被翻译来推断：

  - server request pool /dev3：CC。
  - server response pool /dev6：NC。
  - client request /dev7：NC，当前正确。
  - client response /dev2：CC。
  - v2 的 request/response descriptor 与 arena 同样遵循这套方向。


### 带宽
nc 读：单 1.6 g/s，多核 15 g/s -- cli --> read warm region
nc写：单 4 g/s，多核 10 g/s   --> cli write req & server write resp