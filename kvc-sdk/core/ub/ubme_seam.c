/* ubme_seam.c — 全仓唯一的 ubme 闭源运行时 seam 实现
 *
 * 平台实证参数原样保留，不自创:
 *   - decoder_idx 恒 1（窗口身份由 tokenid 决定）
 *   - memattr=nc import 的 decoder_flag = 0x63（0x60/0x61 丢 NC 位 0x02）
 *   - ubme_map anchor 进程生命周期保活（shipped runtime 只清理 mapped 态）
 *   - ubme_init 内部 journal sweep 在本进程 export 之前回收死进程残留
 *   - mesh 弹跳恢复: 对端重启后 export id 更新，本进程 import 只能删旧重拉
 *
 * 结构: 符号表 init 时一次绑定；export/import 共用 cub_map_register
 * （map 锚定 → /dev 路径解析 → 段表登记，失败统一回滚）。
 */
#define _GNU_SOURCE
#include "ubme_seam.h"

#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define SEAMLOG(...)                                                    \
    do {                                                                \
        fprintf(stderr, "[ubme-seam] " __VA_ARGS__);                    \
        fputc('\n', stderr);                                            \
    } while (0)

/* ---- partner API 最小面（签名镜像公开头文件，实现在闭源库内） ---- */

#define CUB_DIR_EXPORT 0
#define CUB_DIR_IMPORT 1
#define CUB_MEMATTR_NC 0
#define CUB_MEMATTR_CC 1
#define CUB_STATUS_IDLE 0
#define CUB_STATUS_IMPORT_READY 2
#define CUB_STATUS_MAPPED 3
#define CUB_STATUS_PEER_OFFLINE 4

typedef struct ubme_ctx cub_handle_t;

typedef struct {
    char name[64];
    char size_str[32];
    uint64_t size_bytes;
    int direction;
    int decoder_idx;
    uint32_t decoder_flag;
    uint32_t peer_node_ids[16];
    int peer_node_count;
    int memattr;
} cub_shared_cfg_t;

typedef struct {
    const char *config_path;
    void (*on_node_online)(uint32_t node_id, void *user_data);
    void (*on_node_offline)(uint32_t node_id, void *user_data);
    void *on_mapping_ready;
    void *user_data;
} cub_init_params_t;

typedef int (*cub_init_fn)(const cub_init_params_t *, cub_handle_t **);
typedef int (*cub_destroy_fn)(cub_handle_t *);
typedef int (*cub_add_shared_fn)(cub_handle_t *, const cub_shared_cfg_t *,
                                 const void *import_info /* NULL: PULL */);
typedef int (*cub_map_fn)(cub_handle_t *, const char *name, uint64_t bytes,
                          void **out_addr);
typedef int (*cub_remove_shared_fn)(cub_handle_t *, const char *name);
typedef int (*cub_get_status_fn)(cub_handle_t *, const char *name);
typedef int (*cub_list_peer_exports_fn)(cub_handle_t *, uint32_t node_id,
                                        void *out, uint32_t max,
                                        uint32_t timeout_ms);
typedef uint32_t (*cub_get_local_cna_fn)(cub_handle_t *);
typedef int (*cub_get_export_info_fn)(cub_handle_t *, const char *name,
                                      uint32_t *export_id, uint64_t *uba,
                                      uint32_t *tokenid, uint64_t *size);

/* 符号表: init 时一次绑定，调用点不再各自 dlsym。 */
typedef struct {
    cub_init_fn init;
    cub_destroy_fn destroy;
    cub_add_shared_fn add_shared;
    cub_map_fn map;
    cub_remove_shared_fn remove_shared;
    cub_get_status_fn get_status;
    cub_list_peer_exports_fn list_peer_exports; /* 可选: 旧 runtime 无 */
    cub_get_local_cna_fn get_local_cna;         /* 可选 */
    cub_get_export_info_fn get_export_info;     /* 可选 */
} cub_api_t;

/* ---- 常量 ---- */

#define CUB_RUN_DIR_DEFAULT "/var/run/hpc-redis-ubme"
/* obmm export 粒度（平台 module param），非整数倍 export EINVAL。 */
#define CUB_GRANULARITY (2ull * 1024ull * 1024ull)
#define CUB_PEER_QUERY_TIMEOUT_MS 10000
#define CUB_PEER_POLL_US (50 * 1000)
#define CUB_REIMPORT_SETTLE_SEC 3
#define CUB_REIMPORT_TIMEOUT_MS 60000
#define CUB_REONLINE_WINDOW_SEC 30
#define CUB_MAX_SEGMENTS 16
#define CUB_MAX_PEERS 16

static const char *cub_run_dir(void) {
    const char *e = getenv("UBME_SEAM_RUN_DIR");
    return (e && e[0]) ? e : CUB_RUN_DIR_DEFAULT;
}

/* ---- 全局态（g_cub_lock 保护全部字段） ---- */

typedef struct {
    char name[UBME_SEAM_NAME_MAX];
    char device_path[UBME_SEAM_PATH_MAX];
    uint64_t bytes;
    void *anchor_addr;      /* ubme_map 映射，生命周期保活 */
    uint64_t anchor_bytes;
    int imported;           /* 1 = mesh PULL 而来 */
    int reused;             /* 1 = journal 同机复用（runtime 无本进程条目） */
    uint32_t source_node;   /* import: home node */
    int decoder_idx;        /* import: decoder 槽位 */
} cub_segment_t;

static pthread_mutex_t g_cub_lock = PTHREAD_MUTEX_INITIALIZER;
static void *g_cub_dl;
static cub_handle_t *g_cub_handle;
static cub_api_t g_cub_fn;
static cub_segment_t g_cub_segments[CUB_MAX_SEGMENTS];
static uint32_t g_cub_segment_count;
static uint32_t g_cub_peer_ids[CUB_MAX_PEERS];
static uint32_t g_cub_peer_count;
/* mesh 弹跳闩锁: 启动 30s 后 peer 重上线 = 对端重启过，本进程 import 可能
 * 指向已被回收的 export。由 runtime 在线回调置位，reimport_stale 消费。 */
static volatile int g_cub_peers_reonline;
static time_t g_cub_init_time;
static pid_t g_cub_pid;                /* ctx 所属进程（fork 检测） */
static uint32_t g_cub_reinit_node;     /* fork 后按父参数重建 ctx */
static uint16_t g_cub_reinit_port;
static char g_cub_reinit_peers[256];

static void cub_on_node_online(uint32_t node_id, void *user_data) {
    (void)node_id;
    (void)user_data;
    if (time(NULL) - g_cub_init_time > CUB_REONLINE_WINDOW_SEC)
        g_cub_peers_reonline = 1;
}

static cub_segment_t *cub_find_segment(const char *name) {
    for (uint32_t i = 0; i < g_cub_segment_count; i++)
        if (!strcmp(g_cub_segments[i].name, name))
            return &g_cub_segments[i];
    return NULL;
}

/* 解析一个已映射地址背后的 /dev/obmm_shmdevN 路径。 */
static int cub_path_for_addr(const void *addr, char *out_path, size_t cap) {
    FILE *fp = fopen("/proc/self/maps", "r");
    if (!fp)
        return -1;
    char line[512];
    uintptr_t want = (uintptr_t)addr;
    int found = -1;
    while (fgets(line, sizeof(line), fp)) {
        uintptr_t lo, hi;
        char perms[8], path[400];
        path[0] = '\0';
        if (sscanf(line, "%lx-%lx %7s %*s %*s %*s %399s", &lo, &hi, perms,
                   path) < 3)
            continue;
        if (want >= lo && want < hi && strstr(path, "/dev/obmm_shmdev")) {
            snprintf(out_path, cap, "%s", path);
            found = 0;
            break;
        }
    }
    fclose(fp);
    return found;
}

static void cub_format_size(uint64_t bytes, char out[32]) {
    if (bytes >= (1ULL << 30) && bytes % (1ULL << 30) == 0)
        snprintf(out, 32, "%lluG", (unsigned long long)(bytes >> 30));
    else
        snprintf(out, 32, "%lluM",
                 (unsigned long long)((bytes + (1ULL << 20) - 1) >> 20));
}

static uint64_t cub_snap(uint64_t bytes) {
    return (bytes + CUB_GRANULARITY - 1) & ~(CUB_GRANULARITY - 1);
}

/* peers_spec: "id:ip:port:cna,..."。两遍扫描（先计数后写出），并缓存
 * peer id 供 import 的 source 校验。 */
static int cub_write_peers(FILE *fp, const char *peers_spec) {
    if (!peers_spec || !peers_spec[0])
        return 0;
    unsigned count = 0;
    const char *p = peers_spec;
    for (;;) {
        unsigned id, port;
        char ip[64], cna_s[32];
        int consumed = 0;
        if (sscanf(p, " %u:%63[^:]:%u:%31[^,]%n", &id, ip, &port, cna_s,
                   &consumed) != 4)
            return -1;
        /* cna 进制: /proc/mami/udm/udevice 打印十进制（cna[17]，与 eid 同
         * 口径），本仓 conf/装机 config（ub_node_*.cfg）为 0x 前缀十六进
         * 制（primary_cna = 0x11）。两种都收: 0x 前缀按十六进制，裸值按
         * 十进制——喂错进制会把 decoder dstCNA 指到不存在的节点，第一笔
         * NC 读即触发 fabric 外部中止（实测整机 SError panic）。 */
        char *end = NULL;
        unsigned long cna = strtoul(cna_s, &end, 0);
        if (end == cna_s || *end != '\0' || cna == 0 || cna > 0xffffu)
            return -1;
        count++;
        p += consumed;
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == '\0')
            break;
        return -1;
    }
    fprintf(fp, "[topo]\npeer_count = %u\n", count);
    p = peers_spec;
    for (unsigned i = 1; i <= count; i++) {
        unsigned id, port;
        char ip[64], cna_s[32];
        int consumed = 0;
        if (sscanf(p, " %u:%63[^:]:%u:%31[^,]%n", &id, ip, &port, cna_s,
                   &consumed) != 4)
            return -1;
        unsigned long cna = strtoul(cna_s, NULL, 0);
        if (g_cub_peer_count < CUB_MAX_PEERS)
            g_cub_peer_ids[g_cub_peer_count++] = id;
        fprintf(fp,
                "peer.%u.id = %u\npeer.%u.ip = %s\n"
                "peer.%u.tcp_port = %u\npeer.%u.cna = 0x%lx\n",
                i, id, i, ip, i, port, i, cna);
        p += consumed;
        if (*p == ',')
            p++;
    }
    return 0;
}

/* ---- ctx 生命周期 ---- */

/* init 主体（dl/fn 已就绪）: 写 conf + 建运行库 ctx + 记录 fork 重建参数。
 * 调用方持有 g_cub_lock。 */
static int cub_init_locked(uint32_t node_id, uint16_t tcp_port,
                           const char *peers_spec) {
    char conf_path[320];
    snprintf(conf_path, sizeof(conf_path), "%s/ubme_%ld.conf", cub_run_dir(),
             (long)getpid());

    FILE *fp = fopen(conf_path, "w");
    if (!fp) {
        SEAMLOG("cannot write %s: %s", conf_path, strerror(errno));
        return -1;
    }
    /* tcp_port 0 = 内核自动分配（reader ctx；mesh 对 reader 纯出向）。 */
    fprintf(fp, "[local]\nnode_id = %u\ntcp_port = %u\n", (unsigned)node_id,
            (unsigned)tcp_port);
    if (cub_write_peers(fp, peers_spec) != 0) {
        fclose(fp);
        SEAMLOG("bad peers spec: %s", peers_spec ? peers_spec : "(null)");
        return -1;
    }
    fclose(fp);

    cub_init_params_t params = {
        .config_path = conf_path,
        .on_node_online = cub_on_node_online,
    };
    cub_handle_t *handle = NULL;
    g_cub_init_time = time(NULL);
    /* ubme_init 内部先跑 journal sweep: 上一个崩溃进程泄漏的段在此被
     * unexport，之后才是本进程自己的 export/import。 */
    int rc = g_cub_fn.init(&params, &handle);
    if (rc != 0 || !handle) {
        SEAMLOG("ubme_init rc=%d", rc);
        return -1;
    }

    g_cub_handle = handle;
    g_cub_pid = getpid();
    g_cub_reinit_node = node_id;
    g_cub_reinit_port = tcp_port;
    snprintf(g_cub_reinit_peers, sizeof(g_cub_reinit_peers), "%s",
             peers_spec ? peers_spec : "");
    return 0;
}

/* fork 守卫: 子进程继承的 ctx 句柄不可用（运行库后台线程/连接只存在于
 * 父进程，句柄操作语义未定义——实测 fork 型 driver 32 worker 下直读路径
 * 整体劣化为 TE）。检测到 pid 变化即重置本进程状态并按父参数重建 ctx:
 * 不调 destroy（那会动父进程的内核资源）; 子进程段表清零（继承的 mmap
 * 仍有效，无需重 attach; journal 复用按需现取）。 */
static void cub_fork_reinit(void) {
    if (!g_cub_handle || g_cub_pid == getpid())
        return;
    pthread_mutex_lock(&g_cub_lock);
    if (g_cub_handle && g_cub_pid != getpid()) {
        SEAMLOG("fork detected (%d -> %d): rebuilding ctx in child",
                (int)g_cub_pid, (int)getpid());
        g_cub_handle = NULL;
        g_cub_segment_count = 0;
        g_cub_peer_count = 0;
        g_cub_peers_reonline = 0;
        cub_init_locked(g_cub_reinit_node, g_cub_reinit_port,
                        g_cub_reinit_peers);
    }
    pthread_mutex_unlock(&g_cub_lock);
}

int ubme_seam_init(uint32_t node_id, uint16_t tcp_port,
                   const char *peers_spec) {
    pthread_mutex_lock(&g_cub_lock);
    if (g_cub_handle) { /* 幂等重入 */
        pthread_mutex_unlock(&g_cub_lock);
        return 0;
    }

    const char *lib = getenv("UBME_SEAM_LIB");
    lib = (lib && lib[0]) ? lib : "libubme.so";
    g_cub_dl = dlopen(lib, RTLD_NOW | RTLD_LOCAL);
    if (!g_cub_dl) {
        SEAMLOG("cannot load %s (%s); ubme seam requires the partner runtime",
                lib, dlerror());
        goto fail;
    }

    /* 必需符号一次绑定进 g_cub_fn（调用点不再各自 dlsym）。 */
    g_cub_fn.init = (cub_init_fn)dlsym(g_cub_dl, "ubme_init");
    g_cub_fn.destroy = (cub_destroy_fn)dlsym(g_cub_dl, "ubme_destroy");
    g_cub_fn.add_shared =
        (cub_add_shared_fn)dlsym(g_cub_dl, "ubme_add_shared");
    g_cub_fn.map = (cub_map_fn)dlsym(g_cub_dl, "ubme_map");
    g_cub_fn.remove_shared =
        (cub_remove_shared_fn)dlsym(g_cub_dl, "ubme_remove_shared");
    g_cub_fn.get_status =
        (cub_get_status_fn)dlsym(g_cub_dl, "ubme_get_status");
    g_cub_fn.list_peer_exports =
        (cub_list_peer_exports_fn)dlsym(g_cub_dl, "ubme_list_peer_exports");
    g_cub_fn.get_local_cna =
        (cub_get_local_cna_fn)dlsym(g_cub_dl, "ubme_get_local_cna");
    g_cub_fn.get_export_info =
        (cub_get_export_info_fn)dlsym(g_cub_dl, "ubme_get_export_info");
    if (!g_cub_fn.init || !g_cub_fn.destroy || !g_cub_fn.add_shared ||
        !g_cub_fn.map || !g_cub_fn.remove_shared || !g_cub_fn.get_status) {
        SEAMLOG("%s misses required symbols (init/destroy/add_shared/map/"
                "remove_shared/get_status)",
                lib);
        goto fail;
    }

    /* conf 按进程隔离: 同机多 ctx（owner + co-located reader/probe）各写
     * 各的，避免互相覆盖；端口仍须互异（0 = 内核自动分配）。 */
    if (mkdir(cub_run_dir(), 0755) != 0 && errno != EEXIST) {
        SEAMLOG("mkdir %s: %s", cub_run_dir(), strerror(errno));
        goto fail;
    }
    if (cub_init_locked(node_id, tcp_port, peers_spec) != 0)
        goto fail;
    pthread_mutex_unlock(&g_cub_lock);
    SEAMLOG("runtime '%s' active (node_id=%u tcp_port=%u peers=%s)", lib,
            (unsigned)node_id, (unsigned)tcp_port,
            (peers_spec && peers_spec[0]) ? peers_spec : "none");
    return 0;

fail:
    if (g_cub_dl) {
        dlclose(g_cub_dl);
        g_cub_dl = NULL;
    }
    g_cub_fn = (cub_api_t){0};
    g_cub_peer_count = 0;
    pthread_mutex_unlock(&g_cub_lock);
    return -1;
}

void ubme_seam_shutdown(void) {
    cub_fork_reinit();
    pthread_mutex_lock(&g_cub_lock);
    if (g_cub_handle) {
        SEAMLOG("shutdown (unexport all)");
        g_cub_fn.destroy(g_cub_handle);
        g_cub_handle = NULL;
        g_cub_segment_count = 0;
        g_cub_peer_count = 0;
        g_cub_peers_reonline = 0;
    }
    if (g_cub_dl) {
        dlclose(g_cub_dl);
        g_cub_dl = NULL;
    }
    pthread_mutex_unlock(&g_cub_lock);
}

/* ---- 段表登记（export/import/reuse 共用） ---- */

static cub_segment_t *cub_seg_claim(void) {
    if (g_cub_segment_count >= CUB_MAX_SEGMENTS)
        return NULL;
    return &g_cub_segments[g_cub_segment_count++];
}

static void cub_seg_fill(cub_segment_t *seg, const char *name,
                         const char *path, uint64_t bytes, void *anchor,
                         uint64_t anchor_bytes, int imported, int reused,
                         uint32_t source_node) {
    snprintf(seg->name, sizeof(seg->name), "%s", name);
    snprintf(seg->device_path, sizeof(seg->device_path), "%s", path);
    seg->bytes = bytes;
    seg->anchor_addr = anchor;
    seg->anchor_bytes = anchor_bytes;
    seg->imported = imported;
    seg->reused = reused;
    seg->source_node = source_node;
    seg->decoder_idx = 1;
}

static void cub_seg_drop(cub_segment_t *seg) {
    uint32_t idx = (uint32_t)(seg - g_cub_segments);
    memmove(&g_cub_segments[idx], &g_cub_segments[idx + 1],
            (g_cub_segment_count - idx - 1) * sizeof(cub_segment_t));
    g_cub_segment_count--;
}

/* create/import 共同尾部: map 锚定 → /dev 路径解析 → 段表登记。
 * 任一步失败 remove_shared 回滚并返回 -1（调用方持有锁、已预留表项）。 */
static int cub_map_register(const char *name, uint64_t bytes,
                            int imported, int reused, uint32_t source_node,
                            char *out_path, size_t out_cap) {
    void *anchor = NULL;
    if (g_cub_fn.map(g_cub_handle, name, bytes, &anchor) != 0 || !anchor) {
        SEAMLOG("map '%s' failed", name);
        goto fail;
    }
    char path[UBME_SEAM_PATH_MAX];
    if (cub_path_for_addr(anchor, path, sizeof(path)) != 0) {
        SEAMLOG("cannot resolve device for '%s'", name);
        goto fail;
    }
    cub_seg_fill(cub_seg_claim(), name, path, bytes, anchor, bytes,
                 imported, reused, source_node);
    snprintf(out_path, out_cap, "%s", path);
    return 0;

fail:
    g_cub_fn.remove_shared(g_cub_handle, name);
    return -1;
}

/* ---- owner: export 动态段 ---- */

int ubme_seam_create_segment(const char *name, uint64_t bytes, char *out_path,
                             size_t out_cap, uint64_t *actual_bytes) {
    cub_fork_reinit();
    if (!name || !name[0] || bytes == 0 || !out_path)
        return -1;

    pthread_mutex_lock(&g_cub_lock);
    if (!g_cub_handle || g_cub_segment_count >= CUB_MAX_SEGMENTS) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }

    uint64_t export_bytes = cub_snap(bytes);
    if (export_bytes < bytes) { /* 溢出 */
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }

    cub_shared_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    snprintf(cfg.name, sizeof(cfg.name), "%s", name);
    cub_format_size(export_bytes, cfg.size_str);
    cfg.size_bytes = export_bytes;
    cfg.direction = CUB_DIR_EXPORT;
    cfg.memattr = CUB_MEMATTR_CC;

    if (g_cub_fn.add_shared(g_cub_handle, &cfg, NULL) != 0) {
        /* 段名已存在 = 运行中重复创建或双 owner 配置错——硬错误。 */
        SEAMLOG("export '%s' (%llu bytes) failed", name,
                (unsigned long long)export_bytes);
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }

    /* 锚定到 mapped 态: (a) 给出可解析设备路径的本地地址; (b) shipped
     * runtime 只在 destroy 时清理 mapped 条目（见头文件 quirk 注）。 */
    if (cub_map_register(name, export_bytes, 0, 0, 0, out_path, out_cap) !=
        0) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }
    if (actual_bytes)
        *actual_bytes = export_bytes;
    pthread_mutex_unlock(&g_cub_lock);

    SEAMLOG("segment '%s' -> %s (%llu bytes)", name, out_path,
            (unsigned long long)export_bytes);
    return 0;
}

/* ---- reader: journal 同机复用 / mesh 发现 + PULL ---- */

/* journal 同机复用: /var/run/ubme 下的 journal 文件里已有本节点进程对该段名的
 * import/export 记录且 /dev 节点仍在 → 直接复用（同一 hpa 再 import 会被
 * 平台以 EEXIST 拒绝）。 */
static int cub_reuse_journal_path(const char *seg_name, char *out_path,
                                  size_t cap) {
    DIR *d = opendir("/var/run/ubme");
    if (!d)
        return -1;
    char want[96];
    int want_len = snprintf(want, sizeof(want), " name=%s id=", seg_name);
    struct dirent *de;
    int found = -1;
    while (found != 0 && (de = readdir(d)) != NULL) {
        size_t nlen = strlen(de->d_name);
        if (nlen < 9 || strcmp(de->d_name + nlen - 8, ".journal") != 0)
            continue;
        char jpath[320];
        snprintf(jpath, sizeof(jpath), "/var/run/ubme/%s", de->d_name);
        FILE *fp = fopen(jpath, "r");
        if (!fp)
            continue;
        char line[256];
        while (fgets(line, sizeof(line), fp)) {
            if ((strncmp(line, "import", 6) != 0 &&
                 strncmp(line, "export", 6) != 0) ||
                strncmp(line + 6, want, (size_t)want_len) != 0)
                continue;
            long id = strtol(line + 6 + want_len, NULL, 10);
            if (id > 0) {
                snprintf(out_path, cap, "/dev/obmm_shmdev%ld", id);
                if (access(out_path, F_OK) == 0)
                    found = 0;
                break;
            }
        }
        fclose(fp);
    }
    closedir(d);
    return found;
}

int ubme_seam_import_segment(const char *name, uint64_t bytes,
                             uint32_t source_node, uint32_t timeout_ms,
                             char *out_path, size_t out_cap) {
    cub_fork_reinit();
    if (!name || !name[0] || !out_path)
        return -1;

    pthread_mutex_lock(&g_cub_lock);
    if (!g_cub_handle || g_cub_segment_count >= CUB_MAX_SEGMENTS) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }

    /* 同机复用优先: co-located owner/读者已持有该段时平台会拒绝重复
     * import，复用其 /dev 节点即可（无需 source 在 peers 里）。
     * bytes==0 表示尺寸未知（attach 探测式调用）——仅复用路径可行。 */
    char path[UBME_SEAM_PATH_MAX];
    if (cub_reuse_journal_path(name, path, sizeof(path)) == 0) {
        cub_seg_fill(cub_seg_claim(), name, path, cub_snap(bytes), NULL, 0,
                     1, 1, source_node);
        snprintf(out_path, out_cap, "%s", path);
        pthread_mutex_unlock(&g_cub_lock);
        SEAMLOG("import '%s': reused co-located node %s", name, path);
        return 0;
    }
    if (bytes == 0) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;   // 无同机记录且尺寸未知 → 只能走显式尺寸的 mesh import
    }

    /* node id 是部署标识（0 合法），按配置的 mesh 校验而非哨兵值。 */
    int peer_known = 0;
    for (uint32_t i = 0; i < g_cub_peer_count; i++)
        if (g_cub_peer_ids[i] == source_node) {
            peer_known = 1;
            break;
        }
    if (!peer_known) {
        pthread_mutex_unlock(&g_cub_lock);
        SEAMLOG("import '%s': source node %u is not in the configured peers",
                name, (unsigned)source_node);
        return -1;
    }

    uint64_t import_bytes = cub_snap(bytes);

    cub_shared_cfg_t cfg;
    memset(&cfg, 0, sizeof(cfg));
    snprintf(cfg.name, sizeof(cfg.name), "%s", name);
    cub_format_size(import_bytes, cfg.size_str);
    cfg.size_bytes = import_bytes;
    cfg.direction = CUB_DIR_IMPORT;
    cfg.peer_node_ids[0] = source_node;
    cfg.peer_node_count = 1;
    cfg.memattr = CUB_MEMATTR_NC;
    cfg.decoder_idx = 1; /* 平台只 provision 槽 1; 窗口身份由 tokenid 决定 */
    /* NC import 平台实证旗标（ub_node_111/112.cfg 全部 0x63; 丢 0x02 位
     * 会导致远端内存弱序读）。 */
    cfg.decoder_flag = 0x63;

    if (g_cub_fn.add_shared(g_cub_handle, &cfg, NULL) != 0) {
        SEAMLOG("import '%s' from node %u failed", name,
                (unsigned)source_node);
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }

    /* 等后台 PULL（50ms 粒度轮询；每次休眠放锁）。 */
    uint32_t waited_ms = 0;
    int status;
    for (;;) {
        status = g_cub_fn.get_status(g_cub_handle, name);
        if (status == CUB_STATUS_IMPORT_READY || status == CUB_STATUS_MAPPED)
            break;
        if (status == CUB_STATUS_PEER_OFFLINE || waited_ms >= timeout_ms) {
            SEAMLOG("import '%s' from node %u not ready (status=%d waited=%ums)",
                    name, (unsigned)source_node, status, waited_ms);
            g_cub_fn.remove_shared(g_cub_handle, name);
            pthread_mutex_unlock(&g_cub_lock);
            return -1;
        }
        pthread_mutex_unlock(&g_cub_lock);
        usleep(CUB_PEER_POLL_US);
        waited_ms += CUB_PEER_POLL_US / 1000;
        pthread_mutex_lock(&g_cub_lock);
        if (!g_cub_handle) { /* shutdown 竞争 */
            pthread_mutex_unlock(&g_cub_lock);
            return -1;
        }
    }

    if (cub_map_register(name, import_bytes, 1, 0, source_node, out_path,
                         out_cap) != 0) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }
    pthread_mutex_unlock(&g_cub_lock);

    SEAMLOG("imported '%s' <- node %u -> %s (%llu bytes)", name,
            (unsigned)source_node, out_path, (unsigned long long)import_bytes);
    return 0;
}

int ubme_seam_attach_segment(const char *seg_name, uint32_t timeout_ms,
                             char *out_path, size_t out_cap) {
    cub_fork_reinit();
    if (!seg_name || !seg_name[0])
        return -1;
    /* 同机优先: journal 复用（尺寸未知传 0, mesh 无关）。 */
    if (ubme_seam_import_segment(seg_name, 0, 0, timeout_ms, out_path,
                                 out_cap) == 0)
        return 0;
    /* 跨节点: 段名精确匹配经 mesh 发现后 PULL。 */
    ubme_seam_export_desc_t descs[16];
    int found = -1;
    uint32_t node = 0;
    uint64_t size = 0;
    pthread_mutex_lock(&g_cub_lock);
    uint32_t peers[CUB_MAX_PEERS];
    uint32_t n = g_cub_peer_count < CUB_MAX_PEERS ? g_cub_peer_count
                                                  : CUB_MAX_PEERS;
    memcpy(peers, g_cub_peer_ids, n * sizeof(peers[0]));
    cub_list_peer_exports_fn list_fn = g_cub_fn.list_peer_exports;
    cub_handle_t *handle = g_cub_handle;
    pthread_mutex_unlock(&g_cub_lock);
    if (handle && list_fn) {
        size_t plen = strlen(seg_name);
        for (uint32_t i = 0; i < n && found == -1; i++) {
            int rc = list_fn(handle, peers[i], descs, 16,
                             CUB_PEER_QUERY_TIMEOUT_MS);
            for (int j = 0; rc > 0 && j < rc; j++) {
                if (strncmp(descs[j].name, seg_name, plen) != 0)
                    continue;
                found = 0;
                node = peers[i];
                size = descs[j].size;
                break;
            }
        }
    }
    if (found != 0)
        return -1;
    return ubme_seam_import_segment(seg_name, size, node, timeout_ms,
                                    out_path, out_cap);
}

int ubme_seam_query_peer_exports(uint32_t node_id,
                                 ubme_seam_export_desc_t *out, uint32_t max) {
    cub_fork_reinit();
    if (!out || max == 0)
        return 0;
    pthread_mutex_lock(&g_cub_lock);
    cub_handle_t *handle = g_cub_handle;
    cub_list_peer_exports_fn list_fn = g_cub_fn.list_peer_exports;
    pthread_mutex_unlock(&g_cub_lock);
    if (!handle || !list_fn)
        return 0;
    int rc = list_fn(handle, node_id, out, max, CUB_PEER_QUERY_TIMEOUT_MS);
    return rc > 0 ? rc : (rc == 0 ? 0 : -1);
}

int ubme_seam_peer_ids(uint32_t *out, uint32_t max) {
    cub_fork_reinit();
    pthread_mutex_lock(&g_cub_lock);
    uint32_t n = g_cub_peer_count < max ? g_cub_peer_count : max;
    for (uint32_t i = 0; i < n; i++)
        out[i] = g_cub_peer_ids[i];
    uint32_t total = g_cub_peer_count;
    pthread_mutex_unlock(&g_cub_lock);
    return (int)total;
}

int ubme_seam_has_segment(const char *name) {
    cub_fork_reinit();
    if (!name || !name[0])
        return 0;
    pthread_mutex_lock(&g_cub_lock);
    int found = cub_find_segment(name) != NULL;
    pthread_mutex_unlock(&g_cub_lock);
    return found;
}

int ubme_seam_segment_path(const char *name, char *out_path, size_t out_cap) {
    cub_fork_reinit();
    if (!name || !name[0] || !out_path)
        return -1;
    pthread_mutex_lock(&g_cub_lock);
    cub_segment_t *seg = cub_find_segment(name);
    if (!seg) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }
    snprintf(out_path, out_cap, "%s", seg->device_path);
    pthread_mutex_unlock(&g_cub_lock);
    return 0;
}

int ubme_seam_segment_info(const char *name, uint32_t *export_id,
                           uint64_t *uba, uint32_t *tokenid, uint64_t *size) {
    cub_fork_reinit();
    if (!name || !name[0])
        return -1;
    pthread_mutex_lock(&g_cub_lock);
    cub_segment_t *seg = cub_find_segment(name);
    if (!seg) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }
    if (size)
        *size = seg->bytes;
    if (export_id) {
        const char *p = strstr(seg->device_path, "obmm_shmdev");
        if (!p) {
            pthread_mutex_unlock(&g_cub_lock);
            return -1;
        }
        *export_id = (uint32_t)strtoul(p + strlen("obmm_shmdev"), NULL, 10);
    }
    if (uba || tokenid) {
        /* introspection API 仅对本地 export 可用；import/复用条目不可得。 */
        if (seg->imported || !g_cub_fn.get_export_info) {
            pthread_mutex_unlock(&g_cub_lock);
            return -1;
        }
        int rc = g_cub_fn.get_export_info(g_cub_handle, name, NULL, uba,
                                          tokenid, NULL);
        pthread_mutex_unlock(&g_cub_lock);
        return rc == 0 ? 0 : -1;
    }
    pthread_mutex_unlock(&g_cub_lock);
    return 0;
}

int ubme_seam_detach_segment(const char *name) {
    cub_fork_reinit();
    if (!name || !name[0])
        return -1;
    pthread_mutex_lock(&g_cub_lock);
    if (!g_cub_handle) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }
    cub_segment_t *seg = cub_find_segment(name);
    if (!seg) {
        pthread_mutex_unlock(&g_cub_lock);
        return -1;
    }
    /* 复用条目不是本进程的 runtime entry，不能 remove_shared（会误删
     * 同机 owner/其他读者的条目）——只删本地表。 */
    if (!seg->reused)
        g_cub_fn.remove_shared(g_cub_handle, name);
    cub_seg_drop(seg);
    pthread_mutex_unlock(&g_cub_lock);
    SEAMLOG("detached '%s'", name);
    return 0;
}

int ubme_seam_imports_stale(void) {
    cub_fork_reinit();
    pthread_mutex_lock(&g_cub_lock);
    if (!g_cub_handle) {
        pthread_mutex_unlock(&g_cub_lock);
        return 0;
    }
    if (g_cub_peers_reonline) {
        pthread_mutex_unlock(&g_cub_lock);
        return 1;
    }
    for (uint32_t i = 0; i < g_cub_segment_count; i++) {
        if (!g_cub_segments[i].imported || g_cub_segments[i].reused)
            continue;
        int status = g_cub_fn.get_status(g_cub_handle,
                                         g_cub_segments[i].name);
        if (status == CUB_STATUS_PEER_OFFLINE || status == CUB_STATUS_IDLE) {
            pthread_mutex_unlock(&g_cub_lock);
            return 1;
        }
    }
    pthread_mutex_unlock(&g_cub_lock);
    return 0;
}

int ubme_seam_reimport_stale(void) {
    cub_fork_reinit();
    /* Pass 1（持锁）: 收集 stale 的非复用 import 并删除其 runtime 条目。 */
    struct {
        char name[UBME_SEAM_NAME_MAX];
        uint32_t source;
        uint64_t bytes;
    } stale[CUB_MAX_SEGMENTS];
    int stale_count = 0;

    pthread_mutex_lock(&g_cub_lock);
    if (!g_cub_handle) {
        pthread_mutex_unlock(&g_cub_lock);
        return 0;
    }
    int reonline = g_cub_peers_reonline;
    g_cub_peers_reonline = 0;
    for (uint32_t i = 0;
         i < g_cub_segment_count && stale_count < CUB_MAX_SEGMENTS; i++) {
        cub_segment_t *seg = &g_cub_segments[i];
        if (!seg->imported || seg->reused)
            continue;
        int status = g_cub_fn.get_status(g_cub_handle, seg->name);
        if (!reonline && status != CUB_STATUS_PEER_OFFLINE &&
            status != CUB_STATUS_IDLE)
            continue;
        snprintf(stale[stale_count].name, sizeof(stale[stale_count].name),
                 "%s", seg->name);
        stale[stale_count].source = seg->source_node;
        stale[stale_count].bytes = seg->bytes;
        stale_count++;
        g_cub_fn.remove_shared(g_cub_handle, seg->name);
        cub_seg_drop(seg);
        i--;
    }
    pthread_mutex_unlock(&g_cub_lock);
    if (stale_count == 0)
        return 0;

    /* Pass 2（放锁）: 重新 PULL；失败 settle 后重试一次（刚重启的 peer
     * mami 子系统可能仍在初始化，表现为瞬时 EIO）。 */
    int refreshed = 0;
    for (int i = 0; i < stale_count; i++) {
        char path[UBME_SEAM_PATH_MAX];
        if (ubme_seam_import_segment(stale[i].name, stale[i].bytes,
                                     stale[i].source,
                                     CUB_REIMPORT_TIMEOUT_MS, path,
                                     sizeof(path)) == 0) {
            refreshed++;
            continue;
        }
        sleep(CUB_REIMPORT_SETTLE_SEC);
        if (ubme_seam_import_segment(stale[i].name, stale[i].bytes,
                                     stale[i].source,
                                     CUB_REIMPORT_TIMEOUT_MS, path,
                                     sizeof(path)) == 0) {
            refreshed++;
            continue;
        }
        SEAMLOG("reimport '%s' from node %u failed", stale[i].name,
                (unsigned)stale[i].source);
    }
    if (refreshed)
        SEAMLOG("reimported %d stale segment(s)", refreshed);
    return refreshed;
}

uint32_t ubme_seam_local_cna(void) {
    pthread_mutex_lock(&g_cub_lock);
    cub_handle_t *handle = g_cub_handle;
    cub_get_local_cna_fn get_cna = g_cub_fn.get_local_cna;
    pthread_mutex_unlock(&g_cub_lock);
    if (!handle || !get_cna)
        return 0;
    return get_cna(handle);
}

