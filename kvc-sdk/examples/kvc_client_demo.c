/* kvc_client_demo.c — 读方接入示例骨架（P0 接口语义演示，实现落地后可编译）
 *
 * 场景: 推理框架 connector 拿到 Mooncake master 的 replica 描述符后，
 *       用 KVC SDK 零拷贝读 KV block。
 */
#include <stdio.h>
#include "kvc/kvc_client.h"

int main(void)
{
    /* 1. attach 并注册要读的 region（P0: 参数来自 manifest/接入方；
     *    P1: kvc_attach 内置握手） */
    kvc_region_set_t *set = kvc_region_set_create();
#if 0  /* 实现落地后启用
    kvc_region_config_t cfg = {
        .region_id = 100, .provider = KVC_PROVIDER_DEVICE,
        .device_path = "/dev/obmm_shmdev5",   // 远端 view
        .mmap_offset = 0, .bytes = 4ULL << 30,
        .block_size = 7 << 20,                // 目标模型 KV page
        .flags = KVC_REGION_O_SYNC_FALLBACK,
    };
    kvc_region_t *r = kvc_region_open_remote(&cfg);
    kvc_region_set_add(set, r);
*/
#endif

    /* 2. master GetReplicaList 的结果 → kvc_handle_t（字段一一对应） */
    kvc_handle_t h = { .offset = 0, .region_id = 100, .block_size = 512 };

    /* 3a. 拷贝读（小 block） */
    char buf[512];
    int rc = kvc_read_handle(&h, set, buf);
    (void)rc;

    /* 3b. 零拷贝读（大 block 直接消费，租约期内有效） */
    const void *view = NULL;
    rc = kvc_deref_handle(&h, set, &view);
    (void)view;

    kvc_region_set_destroy(set);
    printf("kvc client demo skeleton\n");
    return 0;
}
