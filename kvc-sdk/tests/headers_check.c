/* headers_check.c — 公共头文件自包含性检查
 *
 * 编译通过即证明: 头文件只依赖 libc、无内部 include 泄漏、C ABI 独立可用。
 * 同时用 _Static_assert 钉住关键布局约定。
 */
#include "kvc/kvc_client.h"
#include "kvc/kvc_common.h"
#include "kvc/kvc_server.h"

/* 句柄与状态结构体是 wire/接口契约，布局不可漂移 */
_Static_assert(sizeof(kvc_handle_t) == 16, "kvc_handle_t layout");
_Static_assert(sizeof(kvc_region_config_t) > 0, "kvc_region_config_t");

int main(void)
{
    /* 触摸所有 P0 符号，链接阶段留空（本检查只到编译期） */
    kvc_region_config_t cfg = {0};
    cfg.region_id  = 1;
    cfg.provider   = KVC_PROVIDER_DEVICE;
    cfg.bytes      = 1u << 30;
    cfg.block_size = 512;
    (void)cfg;

    kvc_handle_t h = {1, 0, 512};
    (void)h;

    kvc_slot_info_t info = {KVC_SLOT_FREE, 0, 0, 0};
    (void)info;

    (void)kvc_region_open_local;
    (void)kvc_region_open_remote;
    (void)kvc_region_close;
    (void)kvc_region_meta;
    (void)kvc_slot_allocator_create;
    (void)kvc_slot_allocator_destroy;
    (void)kvc_slot_allocator_alloc;
    (void)kvc_slot_allocator_free;
    (void)kvc_slot_allocator_capacity;
    (void)kvc_slot_allocator_used;
    (void)kvc_slot_mark_writing;
    (void)kvc_slot_mark_ready;
    (void)kvc_slot_invalidate;
    (void)kvc_slot_query;
    (void)kvc_region_slot_addr;
    (void)kvc_region_set_create;
    (void)kvc_region_set_destroy;
    (void)kvc_region_set_add;
    (void)kvc_read_handle;
    (void)kvc_deref_handle;
    return 0;
}
