/* kvc_probe.c — region 探针（独立进程观测 KVC region 状态）
 *
 * 用途: Mooncake 适配验证 —— store_kv_bench 写入后，用本工具独立打开同一
 * region，检查 header 几何、slot 状态分布、数据区填充情况。
 *
 * 用法:
 *   ./kvc_probe <device|anon> <bytes> <block> <region_id> [pattern_hex] [dump_slot]
 *   例: ./kvc_probe /dev/obmm_shmdev1 $((512*1024*1024+...)) 4096 1 ab
 *       （pattern: 统计首字节等于 0xab 的 block 占比，模拟 verify 模式填充）
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "kvc/kvc_server.h"

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr,
                "usage: %s <device|anon> <bytes> <block> <region_id> "
                "[pattern_hex] [dump_slot]\n",
                argv[0]);
        return 2;
    }

    kvc_region_config_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    cfg.region_id = (uint32_t)strtoul(argv[4], NULL, 0);
    cfg.block_size = (uint32_t)strtoul(argv[3], NULL, 0);
    cfg.bytes = strtoull(argv[2], NULL, 0);
    if (strcmp(argv[1], "anon") == 0) {
        cfg.provider = KVC_PROVIDER_ANON;
    } else {
        cfg.provider = KVC_PROVIDER_DEVICE;
        snprintf(cfg.device_path, sizeof(cfg.device_path), "%s", argv[1]);
        cfg.flags = KVC_REGION_O_SYNC_FALLBACK;
    }

    /* 非 CREATE 打开 = 只读观测（几何不符报错） */
    kvc_region_t *r = kvc_region_open_remote(&cfg);
    if (!r) {
        printf("open_remote FAILED (region not initialized or geometry mismatch)\n");
        return 1;
    }

    kvc_region_meta_t meta;
    kvc_region_meta(r, &meta);
    printf("region: id=%u block=%u capacity=%u data_off=%llu data_bytes=%llu\n",
           meta.region_id, meta.block_size, meta.capacity_slots,
           (unsigned long long)meta.data_off,
           (unsigned long long)meta.data_bytes);

    /* slot 状态直方图 */
    uint64_t hist[4] = {0, 0, 0, 0};
    for (uint32_t i = 0; i < meta.capacity_slots; i++) {
        kvc_slot_info_t info;
        if (kvc_slot_query(r, i, &info) != KVC_OK)
            break;
        if (info.state < 4)
            hist[info.state]++;
    }
    printf("slots: FREE=%llu WRITING=%llu READY=%llu INVALID=%llu\n",
           (unsigned long long)hist[0], (unsigned long long)hist[1],
           (unsigned long long)hist[2], (unsigned long long)hist[3]);

    /* 数据区填充统计（可选 pattern） */
    int have_pat = argc >= 6;
    unsigned char pat = have_pat ? (unsigned char)strtoul(argv[5], NULL, 16) : 0;
    const unsigned char *base = kvc_region_data_base(r);
    uint64_t nonzero = 0, patmatch = 0;
    for (uint32_t i = 0; i < meta.capacity_slots; i++) {
        const unsigned char *p = base + (uint64_t)i * meta.block_size;
        if (p[0] != 0 || p[meta.block_size - 1] != 0) {
            nonzero++;
            if (have_pat && p[0] == pat && p[meta.block_size / 2] == pat)
                patmatch++;
        }
    }
    printf("data : touched=%llu/%u", (unsigned long long)nonzero,
           meta.capacity_slots);
    if (have_pat)
        printf(" pattern(0x%02x)_match=%llu", pat, (unsigned long long)patmatch);
    printf("\n");

    if (argc >= 7) {
        uint32_t ds = (uint32_t)strtoul(argv[6], NULL, 0);
        const unsigned char *p =
            base + (uint64_t)ds * meta.block_size;
        printf("dump slot %u first 64B:\n", ds);
        for (int i = 0; i < 64; i++) {
            if (i % 32 == 0)
                printf("  %04x: ", i);
            printf("%02x", p[i]);
            if (i % 32 == 31)
                printf("\n");
        }
        kvc_slot_info_t info;
        kvc_slot_query(r, ds, &info);
        printf("  slot state=%u gen=%u seq=%llu key_hash=0x%llx\n", info.state,
               info.owner_gen, (unsigned long long)info.write_seq,
               (unsigned long long)info.key_hash);
    }

    /* probe 以 open_remote 打开但持有写映射；直接 close 释放 */
    cfg.flags &= ~KVC_REGION_CREATE;
    kvc_region_close(r);
    return 0;
}
