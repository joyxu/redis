/* vemb_v16 向量合成生成器 —— TCP (protocol.cpp) 与 aeron (runner) 共用。
 *
 * VADD/vsim 的向量内容与 memtier 通用 object generator 的 value (32B dummy)
 * 无关: 填充常数, TCP/aeron 逐字节一致, 任意 dim 无越界、O(1) 工作量。 */
#ifndef VEMB_V16_VECTOR_GEN_H
#define VEMB_V16_VECTOR_GEN_H

#include <stdint.h>
#include <wchar.h>

static inline void vemb_v16_fill_vector(float *vector, uint32_t dim,
                                        uint32_t global_id)
{
    /* 向量内容对性能/语义无影响: 全向量填充同一常数 (0.25f 位模式)。
     * wmemset 一次写完 (wchar_t=4B=sizeof(float) on Linux), O(1) 工作量。 */
    (void)global_id;
    wmemset((wchar_t *)vector, (wchar_t)0x3E800000u, dim);
}

#endif /* VEMB_V16_VECTOR_GEN_H */
