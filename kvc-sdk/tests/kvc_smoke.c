/* kvc_smoke.c — P0 端到端语义测试
 *
 * 用法:
 *   ./kvc_smoke                          # 匿名内存（单机语义）
 *   ./kvc_smoke <device> <bytes> <block> [region_id]
 *       例: ./kvc_smoke /dev/obmm_shmdev1 $((1<<30)) 4096 1
 *
 * 流程: open_local(CREATE) → allocator 分配 → mark_writing → 写数据 →
 *       mark_ready → client 侧 read_handle/deref_handle 校验 → 状态机边界用例
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvc/kvc_client.h"
#include "../core/layout/kvc_layout.h"
#include "kvc/kvc_server.h"

static int failures = 0;

#define CHECK(cond, msg)                                                    \
    do {                                                                    \
        if (cond) {                                                         \
            printf("  ok   %s\n", msg);                                     \
        } else {                                                            \
            printf("  FAIL %s\n", msg);                                     \
            failures++;                                                     \
        }                                                                   \
    } while (0)

int main(int argc, char **argv)
{
    kvc_region_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.region_id = 1;
    cfg.block_size = 512;
    cfg.bytes = 1u << 20;
    cfg.flags = KVC_REGION_CREATE;

    if (argc >= 4) {
        cfg.provider = KVC_PROVIDER_DEVICE;
        snprintf(cfg.device_path, sizeof(cfg.device_path), "%s", argv[1]);
        cfg.bytes = strtoull(argv[2], NULL, 0);
        cfg.block_size = (uint32_t)strtoul(argv[3], NULL, 0);
        cfg.flags |= KVC_REGION_O_SYNC_FALLBACK;
    } else {
        cfg.provider = KVC_PROVIDER_ANON;
    }
    if (argc >= 5)
        cfg.region_id = (uint32_t)strtoul(argv[4], NULL, 0);

    printf("region: provider=%d path=%s bytes=%llu block=%u id=%u\n",
           cfg.provider, cfg.device_path, (unsigned long long)cfg.bytes,
           cfg.block_size, cfg.region_id);

    /* 1. 建 region */
    kvc_region_t *r = kvc_region_open_local(&cfg);
    CHECK(r != NULL, "open_local(CREATE)");

    kvc_region_meta_t meta;
    CHECK(kvc_region_meta(r, &meta) == KVC_OK && meta.capacity_slots > 0,
          "meta: capacity>0");
    CHECK(meta.data_off >= KVC_LAYOUT_HEADER_SIZE +
              (uint64_t)meta.capacity_slots * KVC_LAYOUT_SLOT_META_SIZE,
          "LAYOUT: data_off >= slot table end (no overlap)");
    printf("       capacity=%u data_off=%llu data_bytes=%llu\n",
           meta.capacity_slots, (unsigned long long)meta.data_off,
           (unsigned long long)meta.data_bytes);
    CHECK(kvc_region_data_base(r) != NULL &&
          kvc_region_data_bytes(r) == meta.data_bytes, "data base/bytes");

    /* 2. slot 分配器（master 视角，纯位图） */
    kvc_region_meta_t mview = {meta.region_id, meta.block_size,
                               meta.capacity_slots, 0, 0};
    kvc_slot_allocator_t *alloc = kvc_slot_allocator_create(&mview);
    CHECK(alloc != NULL, "allocator create");
    uint32_t slot = 0;
    CHECK(kvc_slot_allocator_alloc(alloc, &slot) == KVC_OK, "alloc slot 0");
    uint32_t slot2 = 0;
    CHECK(kvc_slot_allocator_alloc(alloc, &slot2) == KVC_OK && slot2 == 1,
          "alloc slot 1 sequential");

    /* 3. 状态机写路径 */
    CHECK(kvc_slot_mark_writing(r, slot, 0xdeadbeef12345678ULL) == KVC_OK,
          "mark_writing");
    kvc_slot_info_t info;
    kvc_slot_query(r, slot, &info);
    CHECK(info.state == KVC_SLOT_WRITING && info.key_hash == 0xdeadbeef12345678ULL,
          "query WRITING + key_hash");

    unsigned char *data = kvc_region_slot_addr(r, slot);
    CHECK(data != NULL, "slot_addr");
    for (uint32_t i = 0; i < cfg.block_size; i++)
        data[i] = (unsigned char)(i & 0xff);

    CHECK(kvc_slot_mark_ready(r, slot) == KVC_OK, "mark_ready");
    kvc_slot_query(r, slot, &info);
    CHECK(info.state == KVC_SLOT_READY && info.write_seq == 1,
          "query READY + write_seq=1");

    /* 4. client 侧读（同映射注册进 set） */
    kvc_region_set_t *set = kvc_region_set_create();
    CHECK(kvc_region_set_add(set, r) == KVC_OK, "set_add");
    kvc_handle_t h = {.offset = (uint64_t)slot * cfg.block_size,
                      .region_id = cfg.region_id,
                      .block_size = cfg.block_size};

    unsigned char out[8192];
    CHECK(kvc_read_handle(&h, set, out) == KVC_OK, "read_handle");
    int match = 1;
    for (uint32_t i = 0; i < cfg.block_size; i++)
        if (out[i] != (unsigned char)(i & 0xff))
            match = 0;
    CHECK(match, "read_handle bytes match");

    const void *view = NULL;
    CHECK(kvc_deref_handle(&h, set, &view) == KVC_OK && view == (const void *)data,
          "deref_handle returns slot addr");

    /* 5. 边界用例 */
    kvc_handle_t bad = {.offset = 1,
                        .region_id = cfg.region_id,
                        .block_size = cfg.block_size};
    CHECK(kvc_read_handle(&bad, set, out) == KVC_EINVAL, "misaligned offset -> EINVAL");
    bad.offset = (uint64_t)meta.capacity_slots * cfg.block_size;
    CHECK(kvc_read_handle(&bad, set, out) == KVC_ENOENT, "out-of-range offset -> ENOENT");

    CHECK(kvc_slot_mark_ready(r, slot2) == KVC_EINVAL,
          "mark_ready on FREE slot -> EINVAL");
    CHECK(kvc_slot_mark_writing(r, slot, 1) == KVC_ESTALE,
          "double mark_writing -> ESTALE");
    CHECK(kvc_slot_invalidate(r, slot) == KVC_OK, "invalidate");
    CHECK(kvc_read_handle(&h, set, out) == KVC_ESTALE,
          "read after invalidate -> ESTALE");
    CHECK(kvc_slot_mark_writing(r, slot, 42) == KVC_OK,
          "re-mark_writing after invalidate (master restart reuse)");
    CHECK(kvc_slot_allocator_free(alloc, slot) == KVC_OK, "allocator free");

    /* 6. 非 CREATE 重开校验（仅共享设备有意义：匿名重开是全新零内存，
     *    validate 必然失败——那是正确行为） */
    if (cfg.provider == KVC_PROVIDER_DEVICE) {
        kvc_region_config_t cfg2 = cfg;
        cfg2.flags &= ~KVC_REGION_CREATE;
        kvc_region_t *r2 = kvc_region_open_local(&cfg2);
        CHECK(r2 != NULL, "reopen (validate geometry)");
        if (r2)
            kvc_region_close(r2);
    } else {
        printf("  skip reopen (anon provider)\n");
    }

    kvc_region_set_destroy(set);   /* 会 close r */
    kvc_slot_allocator_destroy(alloc);

    /* ============ P1 用例（重新建 region） ============ */
    printf("--- P1 ---\n");
    kvc_region_config_t cfg3 = cfg;
    cfg3.flags |= KVC_REGION_CREATE;
    kvc_region_t *r3 = kvc_region_open_local(&cfg3);
    CHECK(r3 != NULL, "P1: fresh region");

    /* P1-1: 直写 + 客户端读回 */
    unsigned char wbuf[8192];
    for (uint32_t i = 0; i < cfg3.block_size; i++)
        wbuf[i] = (unsigned char)(0xA0 ^ (i & 0xff));
    CHECK(kvc_write_slot_direct(r3, 5, 0xC0FFEEULL, wbuf) == KVC_OK,
          "write_slot_direct(slot 5)");
    kvc_handle_t h5 = {.offset = 5ULL * cfg3.block_size,
                       .region_id = cfg3.region_id,
                       .block_size = cfg3.block_size};
    kvc_region_set_t *set3 = kvc_region_set_create();
    kvc_region_set_add(set3, r3);
    unsigned char rbuf[8192];
    CHECK(kvc_read_handle(&h5, set3, rbuf) == KVC_OK &&
          memcmp(rbuf, wbuf, cfg3.block_size) == 0,
          "read back direct-written slot");

    /* P1-2: 批量读（3 个 handle，1 个未就绪） */
    kvc_write_slot_direct(r3, 6, 1, wbuf);
    kvc_write_slot_direct(r3, 7, 2, wbuf);
    kvc_slot_mark_writing(r3, 8, 3);   /* 制造一个 WRITING */
    kvc_handle_t hb[3] = {
        {.offset = 6ULL * cfg3.block_size, .region_id = cfg3.region_id,
         .block_size = cfg3.block_size},
        {.offset = 7ULL * cfg3.block_size, .region_id = cfg3.region_id,
         .block_size = cfg3.block_size},
        {.offset = 8ULL * cfg3.block_size, .region_id = cfg3.region_id,
         .block_size = cfg3.block_size}};
    CHECK(kvc_batch_read_handle(hb, 3, set3, rbuf) == 2,
          "batch_read_handle: 2 ok / 1 not-ready");

    /* P1-3: 统计 */
    kvc_region_stats_t st;
    CHECK(kvc_region_stats(r3, &st) == KVC_OK && st.ready_slots == 3 &&
          st.writing_slots == 1,
          "stats: ready=3 writing=1");
    printf("       stats: free=%llu ready=%llu writing=%llu max_seq=%llu\n",
           (unsigned long long)st.free_slots,
           (unsigned long long)st.ready_slots,
           (unsigned long long)st.writing_slots,
           (unsigned long long)st.max_write_seq);

    /* P1-4: 位图重建（READY+WRITING → 已分配 = 4） */
    kvc_region_meta_t m3;
    kvc_region_meta(r3, &m3);
    kvc_slot_allocator_t *a3 = kvc_slot_allocator_create(
        &(kvc_region_meta_t){m3.region_id, m3.block_size, m3.capacity_slots,
                              0, 0});
    CHECK(kvc_slot_allocator_rebuild(a3, r3) == KVC_OK &&
          kvc_slot_allocator_used(a3) == 4,
          "allocator rebuild from states: used=4");

    /* P1-5: 残留回收（WRITING → FREE，重建后 used 应为 3） */
    CHECK(kvc_slot_recover_stale(r3) == 1, "recover_stale: 1 slot");
    CHECK(kvc_slot_allocator_rebuild(a3, r3) == KVC_OK &&
          kvc_slot_allocator_used(a3) == 3,
          "rebuild after recovery: used=3");
    CHECK(kvc_read_handle(&hb[2], set3, rbuf) == KVC_ESTALE,
          "recovered slot read -> ESTALE");

    kvc_region_set_destroy(set3);
    kvc_slot_allocator_destroy(a3);

    printf(failures ? "== SMOKE FAILED (%d) ==\n" : "== SMOKE PASSED ==\n",
           failures);
    return failures ? 1 : 0;
}
