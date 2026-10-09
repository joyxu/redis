/* kvc_ub.h — shmdev/匿名内存映射（内部头） */
#ifndef KVC_UB_H
#define KVC_UB_H

#include <stddef.h>
#include <stdint.h>

/* 映射一块共享内存。path == NULL → 匿名 MAP_SHARED（单机/测试）。
 * 设备路径: open(O_RDWR)，EPERM 时回退 O_RDWR|O_SYNC（远端 noncacheable
 * import 需要，实测验证）。失败返回 NULL。 */
void *kvc_map_memory(const char *path, uint64_t offset, uint64_t bytes,
                     uint32_t flags);

/* 解除映射。addr/bytes 必须与映射时一致。 */
int kvc_unmap_memory(void *addr, uint64_t bytes);

/* 远端(nc)内存专用拷贝: 8 深度独立 load 流水 + posted store。
 * 实测 HW01←HW02 nc 映射: glibc memcpy 单线程 63MB/s（不流水），
 * 本例程单线程 ~1.9GB/s、16 线程 ~13GB/s。cached 源上等价普通拷贝。 */
void kvc_copy_remote(void *dst, const void *src, size_t n);

#endif /* KVC_UB_H */
