/* kvc_region_internal.h — kvc_region_t 内部结构（server/client 实现共用，不对外） */
#ifndef KVC_REGION_INTERNAL_H
#define KVC_REGION_INTERNAL_H

#include "kvc/kvc_server.h"
#include "kvc_layout.h"

struct kvc_region {
    kvc_region_config_t cfg;      /* 建立时的配置 */
    void      *base;              /* 映射基址 */
    uint32_t   capacity_slots;
    uint64_t   data_off;
};

#endif /* KVC_REGION_INTERNAL_H */
