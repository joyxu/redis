/* kvc_client.h — KVC SDK 客户端接口（P0）: 读方的两条路径
 *
 * 使用方: 推理框架 connector（拿到 Mooncake master 的 GetReplicaList 结果后）
 *
 * P0 定位: client 不做路由（位置由 Mooncake master / 接入方给出），
 * 只做"凭句柄读": region_set 管理已 attach 的 region，read/deref 按
 * handle 定位 slot。
 */
#ifndef KVC_CLIENT_H
#define KVC_CLIENT_H

#include "kvc_common.h"
#include "kvc_server.h"   /* kvc_region_t: region 句柄两 SDK 共用（server 建表，client 读表） */

#ifdef __cplusplus
extern "C" {
#endif

/* ---------------------------------------------------------------------------
 * region 注册表 — handle → region 映射的管理
 *
 * 推理节点把要读的 region 逐个 open_remote 后注册进来;
 * read/deref 用 handle.region_id 在表内查找。
 * P0: 线性/简单哈希查找，读写比低频（attach 一次性）。
 * ------------------------------------------------------------------------- */
typedef struct kvc_region_set kvc_region_set_t;

kvc_region_set_t *kvc_region_set_create(void);
void  kvc_region_set_destroy(kvc_region_set_t *set);

/* 注册一个已打开的 region（region_id 冲突返回 KVC_EINVAL）。
 * region 的所有权移交 set，destroy 时统一 close。 */
int   kvc_region_set_add(kvc_region_set_t *set, kvc_region_t *region);

/* ---------------------------------------------------------------------------
 * 读路径
 * ------------------------------------------------------------------------- */

/* 拷贝读: handle → region 查找 → slot 就绪检查（query, 未就绪返回
 * KVC_ENOTREADY）→ memcpy block_size 字节到 out_block。
 * 前置条件: out_block 容量 >= handle.block_size。
 * 返回: KVC_OK / KVC_ENOENT(region 未注册) / KVC_ENOTREADY / KVC_ESTALE */
int kvc_read_handle(const kvc_handle_t *handle, kvc_region_set_t *set,
                    void *out_block);

/* 零拷贝读: 同上，但不拷贝，直接返回 slot 数据指针。
 * 生存期: 到该 slot 下一次 invalidate/覆写为止；持有期间调用方应保证
 * 不触发驱逐（Mooncake 读租约即为此设计）。适合大 block 直接消费。 */
int kvc_deref_handle(const kvc_handle_t *handle, kvc_region_set_t *set,
                     const void **out_ptr);

/* P1: 批量拷贝读。out_blocks 为 n 个连续 block（第 i 个起始于
 * out_blocks + i * hs[i].block_size，各 handle 可不同 block_size）。
 * 返回成功个数；失败（非 READY/未注册）的 block 对应位置不写。 */
uint32_t kvc_batch_read_handle(const kvc_handle_t *hs, uint32_t n,
                               kvc_region_set_t *set, void *out_blocks);

#ifdef __cplusplus
}
#endif

#endif /* KVC_CLIENT_H */
