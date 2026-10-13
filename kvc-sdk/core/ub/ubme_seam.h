/* ubme_seam.h — 全仓唯一的 ubme 闭源运行时 seam（平台基础设施）
 *
 * 任何需要 ubme 动态段的组件（kvc-sdk、src/ VEMB、后续接入方）都经由
 * 本模块 dlopen libubme.so，运行时知识（符号绑定、conf 生成、journal
 * 语义、2MiB 粒度、decoder 参数、设备路径解析）只存在于 ubme_seam.c。
 *
 * 生命周期: init（幂等）→ create/import/attach … → shutdown。
 * 进程内单 ctx；同机多进程各自持 ctx（conf 按 pid 隔离，监听端口须互异，
 * reader 可传 tcp_port=0 由内核自动分配）。
 *
 * 段生命周期契约: 动态段不跨进程死亡——优雅退出 shutdown（unexport + 删
 * journal）；kill -9 残留由下一次 init 内部的 journal sweep 回收。
 */
#ifndef UBME_SEAM_H
#define UBME_SEAM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define UBME_SEAM_PATH_MAX 256
#define UBME_SEAM_NAME_MAX 64

/* 对端导出描述（镜像 partner 公开布局）。 */
typedef struct {
    char name[64];
    uint64_t size;
    uint64_t uba;
    uint32_t tokenid;
    uint32_t export_id;
} ubme_seam_export_desc_t;

/* 一次性初始化。peers_spec: "id:ip:port:cna,..."（本节点视角）或 NULL；
 * tcp_port 0 = 内核自动分配（纯出向 mesh 的 reader ctx）。失败返回 -1。
 * cna 进制: 裸值按十进制（/proc/mami/udm/udevice 打印口径，如 "17"），
 * "0x" 前缀按十六进制（装机 config 口径，如 "0x11"，与前者同一节点）。
 * 喂错进制会把 decoder dstCNA 指到不存在的节点，第一笔 NC 读触发
 * fabric 外部中止（实测可致整机 panic），务必与 /proc 核对。 */
int ubme_seam_init(uint32_t node_id, uint16_t tcp_port,
                   const char *peers_spec);

/* 优雅关停: unexport 本进程全部段并删除 journal 记录。
 * 前置条件: 调用方已解除自己对段设备路径的全部 mmap。 */
void ubme_seam_shutdown(void);

/* ---- owner: export 动态段 ---- */

/* 创建（export）至少 bytes 的段，返回本地设备路径与 2MiB 对齐后的实际
 * 尺寸（调用方必须用 actual_bytes 做布局解算）。段名已存在（双 owner）
 * 返回 -1。 */
int ubme_seam_create_segment(const char *name, uint64_t bytes, char *out_path,
                             size_t out_cap, uint64_t *actual_bytes);

/* ---- reader: mesh 发现 + PULL import ---- */

/* attach 惯用入口: journal 同机复用优先（无需 peers），未命中再按精确
 * 段名经 mesh 发现 + PULL。返回本地设备路径。 */
int ubme_seam_attach_segment(const char *seg_name, uint32_t timeout_ms,
                             char *out_path, size_t out_cap);

/* 显式 import（source 节点必须在 init peers 中；bytes==0 仅同机复用可行）。 */
int ubme_seam_import_segment(const char *name, uint64_t bytes,
                             uint32_t source_node, uint32_t timeout_ms,
                             char *out_path, size_t out_cap);

/* 枚举对端节点的全部 exports（mesh RPC）。返回条目数（>0）、0 无
 * runtime/API、-1 查询错误。 */
int ubme_seam_query_peer_exports(uint32_t node_id,
                                 ubme_seam_export_desc_t *out, uint32_t max);

/* 配置的 mesh peer id 列表。返回实际 peer 数。 */
int ubme_seam_peer_ids(uint32_t *out, uint32_t max);

/* ---- 本进程已持有段的查询与解除 ---- */

int ubme_seam_has_segment(const char *name);
int ubme_seam_segment_path(const char *name, char *out_path, size_t out_cap);

/* 段元数据: export_id 由设备路径解析；uba/tokenid 仅本地 export 可得
 * （introspection API，旧 runtime/复用/import 条目返回 -1）。 */
int ubme_seam_segment_info(const char *name, uint32_t *export_id,
                           uint64_t *uba, uint32_t *tokenid, uint64_t *size);

/* 解除持有（export/import 条目删除；复用条目仅删本地表）。前置条件:
 * 调用方已解除自己的 mmap。owner 重启后的重 attach 前必须调用。 */
int ubme_seam_detach_segment(const char *name);

/* ---- 对端重启恢复 ---- */

/* 非破坏探测: 任一 import stale（peer offline/idle 或 mesh 重上线闩锁）
 * 返回 1。 */
int ubme_seam_imports_stale(void);

/* 删除 stale import 并重新 PULL（每次失败 settle 后重试一次）。
 * 返回刷新的段数；调用方必须对刷新段重新 mmap。 */
int ubme_seam_reimport_stale(void);

/* 本地 fabric CNA（introspection API；不可用返回 0）。 */
uint32_t ubme_seam_local_cna(void);

#ifdef __cplusplus
}
#endif

#endif /* UBME_SEAM_H */
