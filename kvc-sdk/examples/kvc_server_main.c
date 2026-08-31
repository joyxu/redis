/* kvc_server_main.c — 持段方参考进程骨架（实现落地后可编译）
 *
 * 场景: mooncake_client 侧（或独立 daemon）建立 UB region 并挂出。
 * Mooncake 侧由 master 的 TlcSlotAllocator 分配 slot，
 * 写端 TE(protocol="ub") 落数据后翻 READY。
 */
#include <stdio.h>
#include "kvc/kvc_server.h"

int main(void)
{
    kvc_region_config_t cfg = {
        .region_id  = 100,
        .provider   = KVC_PROVIDER_DEVICE,
        .device_path = "/dev/obmm_shmdev1",
        .mmap_offset = 0,
        .bytes      = 1ULL << 30,
        .block_size = 7 << 20,
        .flags      = KVC_REGION_CREATE | KVC_REGION_O_SYNC_FALLBACK,
    };
#if 0  /* 实现落地后启用
    kvc_region_t *r = kvc_region_open_local(&cfg);

    kvc_region_meta_t meta;
    kvc_region_meta(r, &meta);   // → 上报 Mooncake master (MountSegment)

    // 写端路径（由 Mooncake client 驱动）:
    //   PutStart: alloc_slot + mark_writing
    //   TE write: 数据落 slot
    //   PutEnd:   mark_ready
    // 读端: kvc_read_handle / kvc_deref_handle

    kvc_region_close(r);
*/
#endif
    (void)cfg;
    printf("kvc server skeleton\n");
    return 0;
}
