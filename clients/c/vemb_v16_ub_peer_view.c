#define _GNU_SOURCE

#include "vemb_v16_ub_peer_view.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *peer_view_trim(char *text) {
    while (*text && isspace((unsigned char)*text))
        text++;
    char *end = text + strlen(text);
    while (end != text && isspace((unsigned char)end[-1]))
        *--end = '\0';
    return text;
}

static int peer_view_copy_string(char *dst, size_t cap, const char *src) {
    size_t len = strlen(src);
    if (len == 0 || len >= cap)
        return -1;
    memcpy(dst, src, len + 1);
    return 0;
}

static int peer_view_parse_u32(const char *text, uint32_t *out) {
    char *end = NULL;
    unsigned long value = strtoul(text, &end, 10);
    if (end == text || *end != '\0' || value > UINT32_MAX)
        return -1;
    *out = (uint32_t)value;
    return 0;
}

static int peer_view_parse_u64(const char *text, uint64_t *out) {
    char *end = NULL;
    unsigned long long value = strtoull(text, &end, 10);
    if (end == text || *end != '\0')
        return -1;
    *out = (uint64_t)value;
    return 0;
}

static int peer_view_parse_role(const char *text, uint32_t *out) {
    static const struct {
        const char *name;
        uint32_t role;
    } roles[] = {
        {"v1_request_ring", VEMB_V16_UB_PEER_VIEW_V1_REQUEST_RING},
        {"v1_response_ring", VEMB_V16_UB_PEER_VIEW_V1_RESPONSE_RING},
        {"warm_region", VEMB_V16_UB_PEER_VIEW_WARM_REGION},
        {"v2_request_descriptor", VEMB_V16_UB_PEER_VIEW_V2_REQUEST_DESCRIPTOR},
        {"v2_request_arena", VEMB_V16_UB_PEER_VIEW_V2_REQUEST_ARENA},
        {"v2_response_descriptor", VEMB_V16_UB_PEER_VIEW_V2_RESPONSE_DESCRIPTOR},
        {"v2_response_arena", VEMB_V16_UB_PEER_VIEW_V2_RESPONSE_ARENA},
    };
    for (size_t i = 0; i < sizeof(roles) / sizeof(roles[0]); i++) {
        if (!strcmp(text, roles[i].name)) {
            *out = roles[i].role;
            return 0;
        }
    }
    return -1;
}

static int peer_view_parse_bool(const char *text, uint32_t *out) {
    if (!strcmp(text, "true") || !strcmp(text, "yes")) {
        *out = 1;
        return 0;
    }
    if (!strcmp(text, "false") || !strcmp(text, "no")) {
        *out = 0;
        return 0;
    }
    return -1;
}

static int peer_view_role_policy_valid(
        const vemb_v16_ub_peer_view_entry_t *entry) {
    /* Access mode is a deployment property of the mapped path.  In
     * particular, imported dev5..8/dev13..16 are NC even for response
     * resources, while same-host paths may remain CC.  The manifest is the
     * external boundary that declares this choice, so do not impose a
     * role-based cache-policy default here. */
    (void)entry;
    return 1;
}

static int peer_view_entry_valid(const vemb_v16_ub_peer_view_entry_t *entry) {
    return entry->client_host[0] && entry->owner_id != UINT32_MAX &&
        entry->resource_role >= VEMB_V16_UB_PEER_VIEW_V1_REQUEST_RING &&
        entry->resource_role <= VEMB_V16_UB_PEER_VIEW_V2_RESPONSE_ARENA &&
        entry->resource_id[0] && entry->generation != 0 &&
        entry->provider_path[0] && entry->client_path[0] &&
        (entry->map_flags & ~VEMB_V16_UB_PEER_VIEW_MAP_F_FROM_START) == 0 &&
        (entry->cache_policy == VEMB_V16_UB_PEER_VIEW_CACHEABLE ||
         entry->cache_policy == VEMB_V16_UB_PEER_VIEW_NONCACHEABLE) &&
        peer_view_role_policy_valid(entry);
}

static int peer_view_append_entry(vemb_v16_ub_peer_view_manifest_t *manifest,
                                  const vemb_v16_ub_peer_view_entry_t *entry) {
    if (!peer_view_entry_valid(entry) ||
        manifest->entry_count >= VEMB_V16_UB_PEER_VIEW_MAX_ENTRIES)
        return -1;
    for (uint32_t i = 0; i < manifest->entry_count; i++) {
        const vemb_v16_ub_peer_view_entry_t *existing = &manifest->entries[i];
        if (existing->owner_id == entry->owner_id &&
            existing->resource_role == entry->resource_role &&
            existing->generation == entry->generation &&
            !strcmp(existing->client_host, entry->client_host) &&
            !strcmp(existing->provider_path, entry->provider_path))
            return -1;
    }
    manifest->entries[manifest->entry_count++] = *entry;
    return 0;
}

static int peer_view_parse_entry_field(vemb_v16_ub_peer_view_entry_t *entry,
                                       const char *key, const char *value,
                                       uint32_t *seen_fields) {
    enum {
        PEER_VIEW_F_CLIENT_HOST = 1u << 0,
        PEER_VIEW_F_OWNER_ID = 1u << 1,
        PEER_VIEW_F_ROLE = 1u << 2,
        PEER_VIEW_F_RESOURCE_ID = 1u << 3,
        PEER_VIEW_F_GENERATION = 1u << 4,
        PEER_VIEW_F_PROVIDER_PATH = 1u << 5,
        PEER_VIEW_F_CLIENT_PATH = 1u << 6,
        PEER_VIEW_F_MAP_FROM_START = 1u << 7,
        PEER_VIEW_F_CACHE_POLICY = 1u << 8,
    };
    uint32_t bit = 0;
    if (!strcmp(key, "client_host"))
        bit = PEER_VIEW_F_CLIENT_HOST;
    else if (!strcmp(key, "owner_id"))
        bit = PEER_VIEW_F_OWNER_ID;
    else if (!strcmp(key, "resource_role"))
        bit = PEER_VIEW_F_ROLE;
    else if (!strcmp(key, "resource_id"))
        bit = PEER_VIEW_F_RESOURCE_ID;
    else if (!strcmp(key, "generation"))
        bit = PEER_VIEW_F_GENERATION;
    else if (!strcmp(key, "provider_path"))
        bit = PEER_VIEW_F_PROVIDER_PATH;
    else if (!strcmp(key, "client_path"))
        bit = PEER_VIEW_F_CLIENT_PATH;
    else if (!strcmp(key, "map_from_start"))
        bit = PEER_VIEW_F_MAP_FROM_START;
    else if (!strcmp(key, "cache_policy"))
        bit = PEER_VIEW_F_CACHE_POLICY;
    else
        return -1;
    if (*seen_fields & bit)
        return -1;
    *seen_fields |= bit;

    if (!strcmp(key, "client_host"))
        return peer_view_copy_string(entry->client_host,
                                     sizeof(entry->client_host), value);
    if (!strcmp(key, "owner_id"))
        return peer_view_parse_u32(value, &entry->owner_id);
    if (!strcmp(key, "resource_role"))
        return peer_view_parse_role(value, &entry->resource_role);
    if (!strcmp(key, "resource_id"))
        return peer_view_copy_string(entry->resource_id,
                                     sizeof(entry->resource_id), value);
    if (!strcmp(key, "generation"))
        return peer_view_parse_u64(value, &entry->generation);
    if (!strcmp(key, "provider_path"))
        return peer_view_copy_string(entry->provider_path,
                                     sizeof(entry->provider_path), value);
    if (!strcmp(key, "client_path"))
        return peer_view_copy_string(entry->client_path,
                                     sizeof(entry->client_path), value);
    if (!strcmp(key, "map_from_start")) {
        uint32_t enabled = 0;
        if (peer_view_parse_bool(value, &enabled) != 0)
            return -1;
        if (enabled)
            entry->map_flags |= VEMB_V16_UB_PEER_VIEW_MAP_F_FROM_START;
        return 0;
    }
    if (!strcmp(key, "cache_policy")) {
        if (!strcmp(value, "cacheable"))
            entry->cache_policy = VEMB_V16_UB_PEER_VIEW_CACHEABLE;
        else if (!strcmp(value, "noncacheable"))
            entry->cache_policy = VEMB_V16_UB_PEER_VIEW_NONCACHEABLE;
        else
            return -1;
        return 0;
    }
    return -1;
}

int vemb_v16_ub_peer_view_manifest_load(
    const char *path, vemb_v16_ub_peer_view_manifest_t *manifest) {
    if (!path || !path[0] || !manifest)
        return -1;
    FILE *fp = fopen(path, "r");
    if (!fp)
        return -1;

    vemb_v16_ub_peer_view_manifest_t parsed = {0};
    vemb_v16_ub_peer_view_entry_t current = {0};
    uint32_t current_seen_fields = 0;
    int in_entry = 0;
    int saw_version = 0;
    int saw_peer_views = 0;
    char line[1024];
    while (fgets(line, sizeof(line), fp)) {
        char *text = peer_view_trim(line);
        if (!text[0] || text[0] == '#')
            continue;
        if (!strcmp(text, "peer_views:")) {
            if (in_entry || saw_peer_views) {
                fclose(fp);
                return -1;
            }
            saw_peer_views = 1;
            continue;
        }

        if (!strncmp(text, "-", 1)) {
            if (!saw_peer_views || (text[1] && !isspace((unsigned char)text[1])) ||
                (in_entry && ((current_seen_fields & 0xffu) != 0xffu ||
                              peer_view_append_entry(&parsed, &current) != 0))) {
                fclose(fp);
                return -1;
            }
            memset(&current, 0, sizeof(current));
            current.cache_policy = VEMB_V16_UB_PEER_VIEW_CACHEABLE;
            current_seen_fields = 0;
            in_entry = 1;
            text = peer_view_trim(text + 1);
            if (!text[0])
                continue;
        }

        char *colon = strchr(text, ':');
        if (!colon) {
            fclose(fp);
            return -1;
        }
        *colon = '\0';
        char *key = peer_view_trim(text);
        char *value = peer_view_trim(colon + 1);
        size_t value_len = strlen(value);
        if (value_len >= 2 && ((value[0] == '"' && value[value_len - 1] == '"') ||
                               (value[0] == '\'' && value[value_len - 1] == '\''))) {
            value[value_len - 1] = '\0';
            value++;
        }
        if (!in_entry) {
            uint32_t version = 0;
            if (strcmp(key, "version") || saw_version ||
                peer_view_parse_u32(value, &version) != 0 || version != 1) {
                fclose(fp);
                return -1;
            }
            saw_version = 1;
        } else if (peer_view_parse_entry_field(&current, key, value,
                                               &current_seen_fields) != 0) {
            fclose(fp);
            return -1;
        }
    }
    fclose(fp);
    if (!saw_version || !saw_peer_views ||
        (in_entry && ((current_seen_fields & 0xffu) != 0xffu ||
                      peer_view_append_entry(&parsed, &current) != 0)) ||
        parsed.entry_count == 0)
        return -1;
    *manifest = parsed;
    return 0;
}

int vemb_v16_ub_peer_view_manifest_resolve(
    const vemb_v16_ub_peer_view_manifest_t *manifest,
    const char *client_host, uint32_t owner_id,
    vemb_v16_ub_peer_view_resource_role_t resource_role,
    const char *provider_path, uint64_t resource_generation,
    vemb_v16_ub_peer_view_mapping_t *mapping) {
    if (!manifest || !client_host || !client_host[0] || !provider_path ||
        !provider_path[0] || !mapping)
        return -1;
    for (uint32_t i = 0; i < manifest->entry_count; i++) {
        const vemb_v16_ub_peer_view_entry_t *entry = &manifest->entries[i];
        if (entry->owner_id != owner_id ||
            entry->resource_role != (uint32_t)resource_role ||
            strcmp(entry->client_host, client_host) ||
            strcmp(entry->provider_path, provider_path))
            continue;
        if (resource_generation != 0 && entry->generation != resource_generation)
            return -1;
        memcpy(mapping->client_path, entry->client_path,
               sizeof(mapping->client_path));
        memcpy(mapping->resource_id, entry->resource_id,
               sizeof(mapping->resource_id));
        mapping->generation = entry->generation;
        mapping->map_flags = entry->map_flags;
        mapping->cache_policy = entry->cache_policy;
        mapping->local_path = !strcmp(entry->provider_path,
                                      entry->client_path);
        return 0;
    }
    return -1;
}
