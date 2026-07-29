/*
 * Copyright (C) 2011-2017 Redis Labs Ltd.
 *
 * This file is part of memtier_benchmark.
 *
 * memtier_benchmark is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 2.
 *
 * memtier_benchmark is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with memtier_benchmark.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#ifdef HAVE_ASSERT_H
#include <assert.h>
#endif

#include "protocol.h"
#include "memtier_benchmark.h"
#include "libmemcached_protocol/binary.h"

#include "vemb_v16_client_sdk.h"

extern "C" {
void sve_streaming_load_f32(const void *src, void *dst, size_t size);
}

static int vemb_v16_copy_handle_vector(const vemb_v16_resp_t *resp,
                                       const uint8_t *warm_mapped_addr,
                                       uint64_t warm_region_bytes,
                                       char **out_value) {
    if (!resp || !out_value || !warm_mapped_addr || resp->vector_bytes == 0)
        return -1;
    if (resp->vector_offset > warm_region_bytes ||
        resp->vector_bytes > warm_region_bytes - resp->vector_offset) {
        return -1;
    }

    char *value = (char *)malloc(resp->vector_bytes);
    if (!value)
        return -1;

    const uint8_t *src = warm_mapped_addr + resp->vector_offset;
    sve_streaming_load_f32(src, value, resp->vector_bytes);
    *out_value = value;
    return 0;
}

/////////////////////////////////////////////////////////////////////////

abstract_protocol::abstract_protocol() :
    m_read_buf(NULL), m_write_buf(NULL), m_keep_value(false)
{
}

abstract_protocol::~abstract_protocol()
{
}

void abstract_protocol::set_buffers(struct evbuffer* read_buf, struct evbuffer* write_buf)
{
    m_read_buf = read_buf;
    m_write_buf = write_buf;
}

void abstract_protocol::set_keep_value(bool flag)
{
    m_keep_value = flag;
}

/////////////////////////////////////////////////////////////////////////

protocol_response::protocol_response()
    : m_status(NULL), m_mbulk_value(NULL), m_value(NULL), m_value_len(0),
      m_total_len(0), m_hits(0), m_error(false), m_vemb_v16_status(0),
      m_vemb_v16_redirect_owner(UINT32_MAX)
{
}

protocol_response::~protocol_response()
{
    clear();
}

void protocol_response::set_error()
{
    m_error = true;
}

bool protocol_response::is_error(void)
{
    return m_error;
}

void protocol_response::set_vemb_v16_status(uint8_t status,
                                            uint32_t redirect_owner)
{
    m_vemb_v16_status = status;
    m_vemb_v16_redirect_owner = redirect_owner;
}

uint8_t protocol_response::get_vemb_v16_status(void) const
{
    return m_vemb_v16_status;
}

uint32_t protocol_response::get_vemb_v16_redirect_owner(void) const
{
    return m_vemb_v16_redirect_owner;
}

void protocol_response::set_status(const char* status)
{
    if (m_status != NULL)
        free((void *)m_status);
    m_status = status;
}

const char* protocol_response::get_status(void)
{
    return m_status;
}

void protocol_response::set_value(const char* value, unsigned int value_len)
{
    if (m_value != NULL)
        free((void *)m_value);
    m_value = value;
    m_value_len = value_len;
}

const char* protocol_response::get_value(unsigned int* value_len)
{
    assert(value_len != NULL);

    *value_len = m_value_len;
    return m_value;
}

void protocol_response::set_total_len(unsigned int total_len)
{
    m_total_len = total_len;
}

unsigned int protocol_response::get_total_len(void)
{
    return m_total_len;
}

void protocol_response::incr_hits(void)
{
    m_hits++;
}

unsigned int protocol_response::get_hits(void)
{
    return m_hits;
}

void protocol_response::clear(void)
{
    if (m_status != NULL) {
        free((void *)m_status);
        m_status = NULL;
    }
    if (m_value != NULL) {
        free((void *)m_value);
        m_value = NULL;
    }
    if (m_mbulk_value != NULL) {
        delete m_mbulk_value;
        m_mbulk_value = NULL;
    }

    m_value_len = 0;
    m_total_len = 0;
    m_hits = 0;
    m_error = 0;
    m_vemb_v16_status = 0;
    m_vemb_v16_redirect_owner = UINT32_MAX;
}

void protocol_response::set_mbulk_value(mbulk_size_el* element) {
    m_mbulk_value = element;
}

mbulk_size_el* protocol_response::get_mbulk_value() {
    return m_mbulk_value;
}

/////////////////////////////////////////////////////////////////////////

class redis_protocol : public abstract_protocol {
protected:
    enum response_state { rs_initial, rs_read_bulk, rs_read_line, rs_end_bulk };
    response_state m_response_state;
    long m_bulk_len;
    size_t m_response_len;

    unsigned int m_total_bulks_count;
    mbulk_size_el* m_current_mbulk;
    bool m_resp3;
    bool m_attribute;

    bool aggregate_type(char c);
    bool blob_type(char c);
    bool single_type(char c);
    bool response_ended();

public:
    redis_protocol() : m_response_state(rs_initial), m_bulk_len(0), m_response_len(0), m_total_bulks_count(0), m_current_mbulk(NULL), m_resp3(false), m_attribute(false) { }
    virtual redis_protocol* clone(void) { return new redis_protocol(); }
    virtual int select_db(int db);
    virtual int authenticate(const char *credentials);
    virtual int configure_protocol(enum PROTOCOL_TYPE type);
    virtual int write_command_cluster_slots();
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout);
    virtual int parse_response(void);

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command &cmd);
    int write_arbitrary_command(const command_arg *arg);
    int write_arbitrary_command(const char *val, int val_len);
};

int redis_protocol::select_db(int db)
{
    int size = 0;
    char db_str[20];

    snprintf(db_str, sizeof(db_str)-1, "%d", db);
    size = evbuffer_add_printf(m_write_buf,
        "*2\r\n"
        "$6\r\n"
        "SELECT\r\n"
        "$%u\r\n"
        "%s\r\n",
        (unsigned int)strlen(db_str), db_str);
    return size;
}

int redis_protocol::authenticate(const char *credentials)
{
    int size = 0;
    assert(credentials != NULL);

    /* Credentials may be one of:
     * <PASSWORD>           For Redis <6.0 simple AUTH commands.
     * <USER>:<PASSWORD>    For Redis 6.0+ AUTH with both username and password.
     *
     * A :<PASSWORD> will be handled as a special case of quoting a password that
     * contains a colon.
     */

    const char *user = NULL;
    const char *password;

    if (credentials[0] == ':') {
        password = credentials + 1;
    } else {
        password = strchr(credentials, ':');
        if (!password) {
            password = credentials;
        } else {
            user = credentials;
            password++;
        }
    }

    if (!user) {
        size = evbuffer_add_printf(m_write_buf,
            "*2\r\n"
            "$4\r\n"
            "AUTH\r\n"
            "$%zu\r\n"
            "%s\r\n",
            strlen(password), password);
    } else {
        size_t user_len = password - user - 1;
        size = evbuffer_add_printf(m_write_buf,
            "*3\r\n"
            "$4\r\n"
            "AUTH\r\n"
            "$%zu\r\n"
            "%.*s\r\n"
            "$%zu\r\n"
            "%s\r\n",
            user_len,
            (int) user_len,
            user,
            strlen(password),
            password);
    }
    return size;
}

int redis_protocol::configure_protocol(enum PROTOCOL_TYPE type) {
    int size = 0;
    if (type == PROTOCOL_RESP2 || type == PROTOCOL_RESP3) {
        m_resp3 = type == PROTOCOL_RESP3;
        size = evbuffer_add_printf(m_write_buf,
                                   "*2\r\n"
                                   "$5\r\n"
                                   "HELLO\r\n"
                                   "$1\r\n"
                                   "%d\r\n",
                                   type == PROTOCOL_RESP2 ? 2 : 3);
    }
    return size;
}

int redis_protocol::write_command_cluster_slots()
{
    int size = 0;

    size = evbuffer_add(m_write_buf,
                        "*2\r\n"
                        "$7\r\n"
                        "CLUSTER\r\n"
                        "$5\r\n"
                        "SLOTS\r\n",
                        28);

    return size;
}

int redis_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);
    int size = 0;

    if (!expiry && !offset) {
        size = evbuffer_add_printf(m_write_buf,
            "*3\r\n"
            "$3\r\n"
            "SET\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n", value_len);
    } else if(offset) {
        char offset_str[30];
        snprintf(offset_str, sizeof(offset_str)-1, "%u", offset);

        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$8\r\n"
            "SETRANGE\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$%u\r\n", (unsigned int) strlen(offset_str), offset_str, value_len);
    } else {
        char expiry_str[30];
        snprintf(expiry_str, sizeof(expiry_str)-1, "%u", expiry);

        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$5\r\n"
            "SETEX\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$%u\r\n", (unsigned int) strlen(expiry_str), expiry_str, value_len);
    }
    evbuffer_add(m_write_buf, value, value_len);
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += value_len + 2;

    return size;
}

int redis_protocol::write_command_multi_get(const keylist *keylist)
{
    fprintf(stderr, "error: multi get not implemented for redis yet!\n");
    assert(0);
}

int redis_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    int size = 0;

    if (!offset) {
        size = evbuffer_add_printf(m_write_buf,
            "*2\r\n"
            "$3\r\n"
            "GET\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        evbuffer_add(m_write_buf, "\r\n", 2);
        size += key_len + 2;
    } else {
        char offset_str[30];
        snprintf(offset_str, sizeof(offset_str)-1, "%u", offset);

        size = evbuffer_add_printf(m_write_buf,
            "*4\r\n"
            "$8\r\n"
            "GETRANGE\r\n"
            "$%u\r\n", key_len);
        evbuffer_add(m_write_buf, key, key_len);
        size += key_len;
        size += evbuffer_add_printf(m_write_buf,
            "\r\n"
            "$%u\r\n"
            "%s\r\n"
            "$2\r\n"
            "-1\r\n", (unsigned int) strlen(offset_str), offset_str);
    }

    return size;
}

/*
 * Utility function to get the number of digits in a number
 */
static int get_number_length(unsigned int num)
{
    if (num < 10) return 1;
    if (num < 100) return 2;
    if (num < 1000) return 3;
    if (num < 10000) return 4;
    if (num < 100000) return 5;
    if (num < 1000000) return 6;
    if (num < 10000000) return 7;
    if (num < 100000000) return 8;
    if (num < 1000000000) return 9;
    return 10;
}

int redis_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    int size = 0;
    size = evbuffer_add_printf(m_write_buf,
                               "*3\r\n"
                               "$4\r\n"
                               "WAIT\r\n"
                               "$%u\r\n"
                               "%u\r\n"
                               "$%u\r\n"
                               "%u\r\n",
                               get_number_length(num_slaves), num_slaves,
                               get_number_length(timeout), timeout);
    return size;
}

bool redis_protocol::aggregate_type(char c) {
    if (c == '*')
        return true;

    if (m_resp3 && (c == '%' || c == '~' || c == '|'))
        return true;

    return false;
}

bool redis_protocol::blob_type(char c) {
    if (c == '$')
        return true;

    if (m_resp3 && (c == '!' || c == '='))
        return true;

    return false;
}

bool redis_protocol::single_type(char c) {
    if (c == '+' || c == '-' || c == ':')
        return true;

    if (m_resp3 && (c == '_' || c == ',' || c == '#' || c == '('))
        return true;

    return false;
}

bool redis_protocol::response_ended() {
    if (m_total_bulks_count != 0)
        return false;

    if (m_attribute) {
        m_attribute = false;
        return false;
    }

    return true;
}

int redis_protocol::parse_response(void)
{
    char *line;
    size_t res_len;

    while (true) {
        switch (m_response_state) {
            case rs_initial:
                // clear last response
                m_last_response.clear();
                m_response_len = 0;
                m_total_bulks_count = 0;
                m_attribute = 0;
                m_response_state = rs_read_line;

                break;
            case rs_read_line:
                line = evbuffer_readln(m_read_buf, &res_len, EVBUFFER_EOL_CRLF_STRICT);

                // maybe we didn't get it yet?
                if (line == NULL) {
                    return 0;
                }

                // count CRLF
                m_response_len += res_len + 2;

                if (aggregate_type(line[0])) {
                    int count = strtol(line + 1, NULL, 10);

                    // in case of nested mbulk, the mbulk is one of the total bulks
                    if (m_total_bulks_count > 0) {
                        m_total_bulks_count--;
                    }

                    // from bulks counter perspective every count < 0 is equal to 0, because it's not followed by bulks.
                    if (count < 0) {
                        count = 0;
                    }

                    if (line[0] == '|') {
                        // Nested attribute?
                        assert(!m_attribute);

                        m_attribute = true;
                    }

                    // Map or Attribute contain key-value pair
                    if (line[0] == '%' || line[0] == '|') {
                        count *= 2;
                    }

                    if (m_keep_value) {
                        mbulk_size_el* new_mbulk_size = new mbulk_size_el();
                        new_mbulk_size->bulks_count = count;
                        new_mbulk_size->upper_level = m_current_mbulk;

                        // update first mbulk as the response mbulk, or insert it to current mbulk
                        if (m_last_response.get_mbulk_value() == NULL) {
                            m_last_response.set_mbulk_value(new_mbulk_size);
                        } else {
                            m_current_mbulk->add_new_element(new_mbulk_size);
                        }

                        // update current mbulk
                        m_current_mbulk = new_mbulk_size->get_next_mbulk();
                    }

                    m_last_response.set_status(line);
                    m_total_bulks_count += count;

                    if (response_ended()) {
                        m_last_response.set_total_len(m_response_len);
                        m_response_state = rs_initial;
                        return 1;
                    }
                } else if (blob_type(line[0])) {
                    // if it's single bulk (not part of mbulk), we count it here
                    if (m_total_bulks_count == 0) {
                        m_total_bulks_count++;
                    }

                    m_bulk_len = strtol(line + 1, NULL, 10);
                    m_last_response.set_status(line);

                    if (line[0] == '!')
                        m_last_response.set_error();

                    /*
                     * only on negative bulk, the data ends right after the first CRLF ($-1\r\n), so
                     * we skip on rs_read_bulk and jump into rs_end_bulk
                     */
                    if (m_bulk_len < 0) {
                        m_response_state = rs_end_bulk;
                    } else {
                        m_response_state = rs_read_bulk;
                    }
                } else if (single_type(line[0])) {
                    // if it's single bulk (not part of mbulk), we count it here
                    if (m_total_bulks_count == 0) {
                        m_total_bulks_count++;
                    }

                    // if we are not inside mbulk, the status will be kept in m_status anyway
                    if (m_keep_value && m_current_mbulk) {
                        char *bulk_value = strdup(line);
                        assert(bulk_value != NULL);

                        bulk_el* new_bulk = new bulk_el();
                        new_bulk->value = bulk_value;
                        new_bulk->value_len = strlen(bulk_value);

                        // insert it to current mbulk
                        m_current_mbulk->add_new_element(new_bulk);
                        m_current_mbulk = m_current_mbulk->get_next_mbulk();
                    }

                    if (line[0] == '-')
                        m_last_response.set_error();

                    m_last_response.set_status(line);
                    m_total_bulks_count--;

                    if (response_ended()) {
                        m_last_response.set_total_len(m_response_len);
                        m_response_state = rs_initial;
                        return 1;
                    }
                } else {
                    benchmark_debug_log("unsupported response: '%s'.\n", line);
                    free(line);
                    return -1;
                }
                break;
            case rs_read_bulk:
                if (evbuffer_get_length(m_read_buf) >= (unsigned long)(m_bulk_len + 2)) {
                    m_response_len += m_bulk_len + 2;

                    /*
                     * KNOWN ISSUE:
                     * in case of key with zero size (SET X "") that will return $0,
                     * we are not counting it as "hit", and the report will be wrong.
                     * currently this is a limitation because GETRANGE returns $0 for
                     * such key as well as non existing key or existing key without data
                     * in the requested range
                     */
                    if (m_bulk_len > 0) {
                        m_last_response.incr_hits();
                    }

                    m_response_state = rs_end_bulk;
                } else {
                    return 0;
                }
                break;
            case rs_end_bulk:
                if (m_keep_value) {
                    /*
                     * keep bulk value - in case we need to save bulk value it depends
                     * if it's inside a mbulk or not.
                     * in case of receiving just bulk as a response, we save it directly to the m_last_response,
                     * otherwise we insert it to the current mbulk element
                     */
                    char *bulk_value = NULL;
                    int ret;
                    if (m_bulk_len > 0) {
                        bulk_value = (char *) malloc(m_bulk_len);
                        assert(bulk_value != NULL);

                        ret = evbuffer_remove(m_read_buf, bulk_value, m_bulk_len);
                        assert(ret != -1);
                    }

                    // drain last CRLF, zero bulk also includes it ($0\r\n\r\n)
                    if (m_bulk_len >= 0) {
                        ret = evbuffer_drain(m_read_buf, 2);
                        assert(ret != -1);
                    }

                    // in case we are inside mbulk
                    if (m_current_mbulk) {
                        bulk_el* new_bulk = new bulk_el();
                        new_bulk->value = bulk_value;
                        // negative bulk len counted as empty bulk
                        new_bulk->value_len = m_bulk_len > 0 ? m_bulk_len : 0;

                        // insert it to current mbulk
                        m_current_mbulk->add_new_element(new_bulk);
                        m_current_mbulk = m_current_mbulk->get_next_mbulk();
                    } else {
                        // negative bulk len counted as empty bulk
                        m_last_response.set_value(bulk_value, m_bulk_len > 0 ? m_bulk_len : 0);
                    }
                } else {
                    // just drain the buffer, include the CRLF
                    if (m_bulk_len >= 0) {
                        int ret = evbuffer_drain(m_read_buf, m_bulk_len + 2);
                        assert(ret != -1);
                    }
                }

                m_total_bulks_count--;

                if (response_ended()) {
                    m_last_response.set_total_len(m_response_len);
                    m_response_state = rs_initial;
                    return 1;
                } else {
                    m_response_state = rs_read_line;
                }
                break;
            default:
                return -1;
        }
    }

    return -1;
}

int redis_protocol::write_arbitrary_command(const command_arg *arg) {
    evbuffer_add(m_write_buf, arg->data.c_str(), arg->data.length());

    return arg->data.length();
}

int redis_protocol::write_arbitrary_command(const char *rand_val, int rand_val_len) {
    int size = 0;

    size = evbuffer_add_printf(m_write_buf, "$%d\r\n", rand_val_len);
    evbuffer_add(m_write_buf, rand_val, rand_val_len);
    size += rand_val_len;
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += 2;

    return size;
}

bool redis_protocol::format_arbitrary_command(arbitrary_command &cmd) {
    for (unsigned int i = 0; i < cmd.command_args.size(); i++) {
        command_arg* current_arg = &cmd.command_args[i];
        current_arg->type = const_type;

        // check arg type
        if (current_arg->data.find(KEY_PLACEHOLDER) != std::string::npos) {
            if (current_arg->data.length() != strlen(KEY_PLACEHOLDER)) {
                benchmark_error_log("error: key placeholder can't combined with other data\n");
                return false;
            }
            cmd.keys_count++;
            current_arg->type = key_type;
        } else if (current_arg->data.find(DATA_PLACEHOLDER) != std::string::npos) {
            if (current_arg->data.length() != strlen(DATA_PLACEHOLDER)) {
                benchmark_error_log("error: data placeholder can't combined with other data\n");
                return false;
            }

            current_arg->type = data_type;
        }

        // we expect that first arg is the COMMAND name
        assert(i != 0 || (i == 0 && current_arg->type == const_type && "first arg is not command name?"));

        if (current_arg->type == const_type) {
            char buffer[20];
            int buffer_len;

            // if it's first arg we add also the mbulk size
            if (i == 0) {
                buffer_len = snprintf(buffer, 20, "*%zd\r\n$%zd\r\n", cmd.command_args.size(), current_arg->data.length());
            } else {
                buffer_len = snprintf(buffer, 20, "$%zd\r\n", current_arg->data.length());
            }

            current_arg->data.insert(0, buffer, buffer_len);
            current_arg->data += "\r\n";
        }
    }

    return true;
}

/////////////////////////////////////////////////////////////////////////

class memcache_text_protocol : public abstract_protocol {
protected:
    enum response_state { rs_initial, rs_read_section, rs_read_value, rs_read_end };
    response_state m_response_state;
    unsigned int m_value_len;
    size_t m_response_len;
public:
    memcache_text_protocol() : m_response_state(rs_initial), m_value_len(0), m_response_len(0) { }
    virtual memcache_text_protocol* clone(void) { return new memcache_text_protocol(); }
    virtual int select_db(int db);
    virtual int authenticate(const char *credentials);
    virtual int configure_protocol(enum PROTOCOL_TYPE type);
    virtual int write_command_cluster_slots();
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout);
    virtual int parse_response(void);

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command& cmd);
    virtual int write_arbitrary_command(const command_arg *arg);
    virtual int write_arbitrary_command(const char *val, int val_len);

};

int memcache_text_protocol::select_db(int db)
{
    assert(0);
}

int memcache_text_protocol::authenticate(const char *credentials)
{
    assert(0);
}

int memcache_text_protocol::configure_protocol(enum PROTOCOL_TYPE type)
{
    assert(0);
}

int memcache_text_protocol::write_command_cluster_slots()
{
    assert(0);
}

int memcache_text_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);
    int size = 0;

    size = evbuffer_add_printf(m_write_buf,
        "set %.*s 0 %u %u\r\n", key_len, key, expiry, value_len);
    evbuffer_add(m_write_buf, value, value_len);
    evbuffer_add(m_write_buf, "\r\n", 2);
    size += value_len + 2;

    return size;
}

int memcache_text_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    int size = 0;

    size = evbuffer_add_printf(m_write_buf,
        "get %.*s\r\n", key_len, key);
    return size;
}

int memcache_text_protocol::write_command_multi_get(const keylist *keylist)
{
    assert(keylist != NULL);
    assert(keylist->get_keys_count() > 0);

    int n = 0;
    int size = 0;

    n = evbuffer_add(m_write_buf, "get", 3);
    assert(n != -1);
    size = 3;

    for (unsigned int i = 0; i < keylist->get_keys_count(); i++) {
        const char *key;
        unsigned int key_len;

        n = evbuffer_add(m_write_buf, " ", 1);
        assert(n != -1);
        size++;

        key = keylist->get_key(i, &key_len);
        assert(key != NULL);

        n = evbuffer_add(m_write_buf, key, key_len);
        assert(n != -1);
        size += key_len;
    }

    n = evbuffer_add(m_write_buf, "\r\n", 2);
    assert(n != -1);
    size += 2;

    return size;
}

int memcache_text_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    fprintf(stderr, "error: WAIT command not implemented for memcache!\n");
    assert(0);
}

int memcache_text_protocol::parse_response(void)
{
    char *line;
    size_t tmplen;

    while (true) {
        switch (m_response_state) {
            case rs_initial:
                m_last_response.clear();
                m_response_state = rs_read_section;
                m_response_len = 0;
                break;

            case rs_read_section:
                line = evbuffer_readln(m_read_buf, &tmplen, EVBUFFER_EOL_CRLF_STRICT);
                if (!line)
                    return 0;

                m_response_len += tmplen + 2;   // For CRLF
                if (m_last_response.get_status() == NULL) {
                    m_last_response.set_status(line);
                }
                m_last_response.set_total_len((unsigned int) m_response_len);   // for now...

                if (memcmp(line, "VALUE", 5) == 0) {
                    char prefix[50];
                    char key[256];
                    unsigned int flags;
                    unsigned int cas;

                    int res = sscanf(line, "%s %s %u %u %u", prefix, key, &flags, &m_value_len, &cas);
                    if (res < 4|| res > 5) {
                        benchmark_debug_log("unexpected VALUE response: %s\n", line);
                        if (m_last_response.get_status() != line)
                            free(line);
                        return -1;
                    }

                    m_response_state = rs_read_value;
                    continue;
                } else if (memcmp(line, "END", 3) == 0 ||
                           memcmp(line, "STORED", 6) == 0) {
                    if (m_last_response.get_status() != line)
                        free(line);
                    m_response_state = rs_read_end;
                    break;
                } else {
                    m_last_response.set_error();
                    benchmark_debug_log("unknown response: %s\n", line);
                    return -1;
                }
                break;

            case rs_read_value:
                if (evbuffer_get_length(m_read_buf) >= m_value_len + 2) {
                    if (m_keep_value) {
                        char *value = (char *) malloc(m_value_len);
                        assert(value != NULL);

                        int ret = evbuffer_remove(m_read_buf, value, m_value_len);
                        assert((unsigned int) ret == 0);

                        m_last_response.set_value(value, m_value_len);
                    } else {
                        int ret = evbuffer_drain(m_read_buf, m_value_len);
                        assert((unsigned int) ret == 0);
                    }

                    int ret = evbuffer_drain(m_read_buf, 2);
                    assert((unsigned int) ret == 0);

                    m_last_response.incr_hits();
                    m_response_len += m_value_len + 2;
                    m_response_state = rs_read_section;
                } else {
                    return 0;
                }
                break;
            case rs_read_end:
                m_response_state = rs_initial;
                return 1;

            default:
                benchmark_debug_log("unknown response state %d.\n", m_response_state);
                return -1;
        }
    }

    return -1;
}

bool memcache_text_protocol::format_arbitrary_command(arbitrary_command& cmd) {
    assert(0);
}

int memcache_text_protocol::write_arbitrary_command(const command_arg *arg) {
    assert(0);
}

int memcache_text_protocol::write_arbitrary_command(const char *val, int val_len) {
    assert(0);
}

/////////////////////////////////////////////////////////////////////////

class memcache_binary_protocol : public abstract_protocol {
protected:
    enum response_state { rs_initial, rs_read_body };
    response_state m_response_state;
    protocol_binary_response_no_extras m_response_hdr;
    size_t m_response_len;

    const char* status_text(void);
public:
    memcache_binary_protocol() : m_response_state(rs_initial), m_response_len(0) { }
    virtual memcache_binary_protocol* clone(void) { return new memcache_binary_protocol(); }
    virtual int select_db(int db);
    virtual int authenticate(const char *credentials);
    virtual int configure_protocol(enum PROTOCOL_TYPE type);
    virtual int write_command_cluster_slots();
    virtual int write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset);
    virtual int write_command_get(const char *key, int key_len, unsigned int offset);
    virtual int write_command_multi_get(const keylist *keylist);
    virtual int write_command_wait(unsigned int num_slaves, unsigned int timeout);
    virtual int parse_response(void);

    // handle arbitrary command
    virtual bool format_arbitrary_command(arbitrary_command& cmd);
    virtual int write_arbitrary_command(const command_arg *arg);
    virtual int write_arbitrary_command(const char *val, int val_len);
};

int memcache_binary_protocol::select_db(int db)
{
    assert(0);
}

int memcache_binary_protocol::authenticate(const char *credentials)
{
    protocol_binary_request_no_extras req;
    char nullbyte = '\0';
    const char mechanism[] = "PLAIN";
    int mechanism_len = sizeof(mechanism) - 1;
    const char *colon;
    const char *user;
    int user_len;
    const char *passwd;
    int passwd_len;

    assert(credentials != NULL);
    colon = strchr(credentials, ':');
    assert(colon != NULL);

    user = credentials;
    user_len = colon - user;
    passwd = colon + 1;
    passwd_len = strlen(passwd);

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SASL_AUTH;
    req.message.header.request.keylen = htons(mechanism_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(mechanism_len + user_len + passwd_len + 2);
    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, mechanism, mechanism_len);
    evbuffer_add(m_write_buf, &nullbyte, 1);
    evbuffer_add(m_write_buf, user, user_len);
    evbuffer_add(m_write_buf, &nullbyte, 1);
    evbuffer_add(m_write_buf, passwd, passwd_len);

    return sizeof(req) + user_len + passwd_len + 2 + sizeof(mechanism) - 1;
}

int memcache_binary_protocol::configure_protocol(enum PROTOCOL_TYPE type) {
    assert(0);
}

int memcache_binary_protocol::write_command_cluster_slots()
{
    assert(0);
}

int memcache_binary_protocol::write_command_set(const char *key, int key_len, const char *value, int value_len, int expiry, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);
    assert(value != NULL);
    assert(value_len > 0);

    protocol_binary_request_set req;

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_SET;
    req.message.header.request.keylen = htons(key_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(sizeof(req.message.body) + value_len + key_len);
    req.message.header.request.extlen = sizeof(req.message.body);
    req.message.body.expiration = htonl(expiry);

    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, key, key_len);
    evbuffer_add(m_write_buf, value, value_len);

    return sizeof(req) + key_len + value_len;
}

int memcache_binary_protocol::write_command_get(const char *key, int key_len, unsigned int offset)
{
    assert(key != NULL);
    assert(key_len > 0);

    protocol_binary_request_get req;

    memset(&req, 0, sizeof(req));
    req.message.header.request.magic = PROTOCOL_BINARY_REQ;
    req.message.header.request.opcode = PROTOCOL_BINARY_CMD_GET;
    req.message.header.request.keylen = htons(key_len);
    req.message.header.request.datatype = PROTOCOL_BINARY_RAW_BYTES;
    req.message.header.request.bodylen = htonl(key_len);
    req.message.header.request.extlen = 0;

    evbuffer_add(m_write_buf, &req, sizeof(req));
    evbuffer_add(m_write_buf, key, key_len);

    return sizeof(req) + key_len;
}

int memcache_binary_protocol::write_command_multi_get(const keylist *keylist)
{
    fprintf(stderr, "error: multi get not implemented for binary memcache yet!\n");
    assert(0);
}

const char* memcache_binary_protocol::status_text(void)
{
    int status;
    static const char* status_str_00[] = {
        "PROTOCOL_BINARY_RESPONSE_SUCCESS",
        "PROTOCOL_BINARY_RESPONSE_KEY_ENOENT",
        "PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS",
        "PROTOCOL_BINARY_RESPONSE_E2BIG",
        "PROTOCOL_BINARY_RESPONSE_EINVAL",
        "PROTOCOL_BINARY_RESPONSE_NOT_STORED",
        "PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL",
        "PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET",
    };
    static const char* status_str_20[] = {
        "PROTOCOL_BINARY_RESPONSE_AUTH_ERROR",
        "PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE"
    };
    static const char* status_str_80[] = {
        NULL,
        "PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND",
        "PROTOCOL_BINARY_RESPONSE_ENOMEM",
        "PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED",
        "PROTOCOL_BINARY_RESPONSE_EINTERNAL",
        "PROTOCOL_BINARY_RESPONSE_EBUSY",
        "PROTOCOL_BINARY_RESPONSE_ETMPFAIL"
    };

    status = ntohs(m_response_hdr.message.header.response.status);
    if (status <= 0x07) {
        return status_str_00[status];
    } else if (status >= 0x20 && status <= 0x21) {
        return status_str_20[status - 0x20];
    } else if (status >= 0x80 && status <= 0x86) {
        return status_str_80[status - 0x80];
    } else {
        return NULL;
    }
}

int memcache_binary_protocol::write_command_wait(unsigned int num_slaves, unsigned int timeout)
{
    fprintf(stderr, "error: WAIT command not implemented for memcache!\n");
    assert(0);
}

int memcache_binary_protocol::parse_response(void)
{
    while (true) {
        int ret;
        int status;

        switch (m_response_state) {
            case rs_initial:
                if (evbuffer_get_length(m_read_buf) < sizeof(m_response_hdr))
                    return 0;               // no header yet?

                ret = evbuffer_remove(m_read_buf, (void *)&m_response_hdr, sizeof(m_response_hdr));
                assert(ret == sizeof(m_response_hdr));

                if (m_response_hdr.message.header.response.magic != PROTOCOL_BINARY_RES) {
                    benchmark_error_log("error: invalid memcache response header magic.\n");
                    return -1;
                }

                m_response_len = sizeof(m_response_hdr);
                m_last_response.clear();
                if (status_text()) {
                    m_last_response.set_status(strdup(status_text()));
                }

                status = ntohs(m_response_hdr.message.header.response.status);
                if (status == PROTOCOL_BINARY_RESPONSE_AUTH_ERROR ||
                    status == PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE ||
                    status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED ||
                    status == PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND ||
                    status == PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED ||
                    status == PROTOCOL_BINARY_RESPONSE_EBUSY) {
                    m_last_response.set_error();
                }

                if (ntohl(m_response_hdr.message.header.response.bodylen) > 0) {
                    m_response_hdr.message.header.response.bodylen = ntohl(m_response_hdr.message.header.response.bodylen);
                    m_response_hdr.message.header.response.keylen = ntohs(m_response_hdr.message.header.response.keylen);

                    m_response_state = rs_read_body;
                    continue;
                }

                return 1;
                break;
            case rs_read_body:
                if (evbuffer_get_length(m_read_buf) >= m_response_hdr.message.header.response.bodylen) {
                    // get rid of extras and key, we don't care about them
                    ret = evbuffer_drain(m_read_buf,
                        m_response_hdr.message.header.response.extlen +
                        m_response_hdr.message.header.response.keylen);
                    assert((unsigned int) ret == 0);

                    int actual_body_len = m_response_hdr.message.header.response.bodylen -
                        m_response_hdr.message.header.response.extlen -
                        m_response_hdr.message.header.response.keylen;
                    if (m_keep_value) {
                        char *value = (char *) malloc(actual_body_len);
                        assert(value != NULL);
                        ret = evbuffer_remove(m_read_buf, value, actual_body_len);
                        m_last_response.set_value(value, actual_body_len);
                    } else {
                        int ret = evbuffer_drain(m_read_buf, actual_body_len);
                        assert((unsigned int) ret == 0);
                    }

                    if (m_response_hdr.message.header.response.status == PROTOCOL_BINARY_RESPONSE_SUCCESS)
                        m_last_response.incr_hits();

                    m_response_len += m_response_hdr.message.header.response.bodylen;
                    m_response_state = rs_initial;

                    return 1;
                } else {
                    return 0;
                }
                break;
            default:
                benchmark_debug_log("unknown response state.\n");
                return -1;
        }
    }

    return -1;
}

bool memcache_binary_protocol::format_arbitrary_command(arbitrary_command& cmd) {
    assert(0);
}

int memcache_binary_protocol::write_arbitrary_command(const command_arg *arg) {
    assert(0);
}

int memcache_binary_protocol::write_arbitrary_command(const char *val, int val_len) {
    assert(0);
}

/////////////////////////////////////////////////////////////////////////


/////////////////////////////////////////////////////////////////////////
// VEMB V16 TCP Protocol Implementation
/////////////////////////////////////////////////////////////////////////


vemb_v16_protocol::vemb_v16_protocol(uint32_t dim, uint32_t max_vectors)
    : m_channel_id(0), m_req_id(1), m_dim(dim), m_max_vectors(max_vectors),
      m_handle_mode(false),
      m_warm_mapping_addr(NULL), m_warm_mapping_bytes(0),
      m_warm_mapped_addr(NULL), m_warm_region_bytes(0),
      m_vsim_mode(false), m_vsim_query_vector(NULL),
      m_vsim_req_template(NULL), m_vsim_req_template_size(0),
      m_vsim_key_key_mode(false), m_key2_prefix_len(0), m_key2_rng(42),
      m_vrem_mode(false), m_topology_epoch(0), m_next_request_flags(0)
{
}

vemb_v16_protocol::~vemb_v16_protocol()
{
    close_warm_region();
    if (m_vsim_query_vector) {
        free(m_vsim_query_vector);
        m_vsim_query_vector = NULL;
    }
    if (m_vsim_req_template) {
        free(m_vsim_req_template);
        m_vsim_req_template = NULL;
        m_vsim_req_template_size = 0;
    }
}

void vemb_v16_protocol::build_vsim_template(void)
{
    if (!m_vsim_mode || !m_vsim_query_vector || m_channel_id == 0 || m_dim == 0)
        return;

    uint32_t vector_bytes = m_dim * sizeof(float);
    uint32_t payload_len = (uint32_t)vemb_v16_req_inline_len(vector_bytes);
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

    if (m_vsim_req_template) {
        free(m_vsim_req_template);
    }
    m_vsim_req_template = (uint8_t *)malloc(total);
    if (!m_vsim_req_template) {
        m_vsim_req_template_size = 0;
        return;
    }
    m_vsim_req_template_size = total;

    /* Use SDK serializer with dummy key; req_id/key/key_hash patched per-request */
    ssize_t len = vemb_v16_serialize_vsim_inline(m_vsim_req_template, total,
                                                 m_channel_id, 0,
                                                 "", 0,
                                                 m_vsim_query_vector, m_dim);
    if ((size_t)len != total) {
        free(m_vsim_req_template);
        m_vsim_req_template = NULL;
        m_vsim_req_template_size = 0;
    }
}

void vemb_v16_protocol::set_vsim_mode(bool enable)
{
    m_vsim_mode = enable;
    if (enable && m_dim > 0 && !m_vsim_query_vector) {
        m_vsim_query_vector = (float *)malloc(m_dim * sizeof(float));
        if (m_vsim_query_vector) {
            for (uint32_t i = 0; i < m_dim; i++) {
                m_vsim_query_vector[i] = 0.1f;
            }
        }
    }
    if (!enable && m_vsim_req_template) {
        free(m_vsim_req_template);
        m_vsim_req_template = NULL;
        m_vsim_req_template_size = 0;
    }
    if (enable) {
        build_vsim_template();
    }
}

void vemb_v16_protocol::set_vsim_key_key_mode(bool enable)
{
    m_vsim_key_key_mode = enable;
    /* VSIM_KEY_KEY builds requests dynamically per-key, no template needed */
}

void vemb_v16_protocol::set_vrem_mode(bool enable)
{
    m_vrem_mode = enable;
}

void vemb_v16_protocol::set_handle_mode(bool enable)
{
    m_handle_mode = enable;
}

void vemb_v16_protocol::set_topology_epoch(uint64_t topology_epoch)
{
    m_topology_epoch = topology_epoch;
    if (m_vsim_req_template) {
        build_vsim_template();
    }
}

void vemb_v16_protocol::set_next_request_flags(uint8_t flags)
{
    m_next_request_flags = flags;
}

void vemb_v16_protocol::set_dim(uint32_t dim)
{
    if (dim == m_dim || dim == 0) return;
    m_dim = dim;
    if (m_vsim_query_vector) {
        float *nv = (float *)realloc(m_vsim_query_vector, m_dim * sizeof(float));
        if (nv) {
            m_vsim_query_vector = nv;
            for (uint32_t i = 0; i < m_dim; i++) {
                m_vsim_query_vector[i] = 0.1f;
            }
        }
    }
    /* dim changed: rebuild template */
    if (m_vsim_req_template) {
        free(m_vsim_req_template);
        m_vsim_req_template = NULL;
        m_vsim_req_template_size = 0;
    }
    if (m_vsim_mode) {
        build_vsim_template();
    }
}

abstract_protocol* vemb_v16_protocol::clone(void)
{
    vemb_v16_protocol *p = new vemb_v16_protocol(m_dim, m_max_vectors);
    p->set_handle_mode(m_handle_mode);
    p->set_vsim_mode(m_vsim_mode);
    p->set_vsim_key_key_mode(m_vsim_key_key_mode);
    p->set_vrem_mode(m_vrem_mode);
    p->set_topology_epoch(m_topology_epoch);
    return p;
}

void vemb_v16_protocol::close_warm_region(void)
{
    vemb_v16_close_warm_region(m_warm_mapping_addr, m_warm_mapping_bytes);
    m_warm_mapping_addr = NULL;
    m_warm_mapping_bytes = 0;
    m_warm_mapped_addr = NULL;
    m_warm_region_bytes = 0;
}

int vemb_v16_protocol::open_warm_region(const vemb_v16_channel_desc_t *desc)
{
    if (!desc || !desc->vector_region_name[0])
        return -1;

    close_warm_region();

    void *mapping_addr = NULL;
    size_t mapping_bytes = 0;
    void *mapped_addr = NULL;
    uint64_t region_bytes = 0;

    if (vemb_v16_open_warm_region(desc, &mapping_addr, &mapping_bytes,
                                  &mapped_addr, &region_bytes) != 0) {
        return -1;
    }

    m_warm_region_bytes = region_bytes;
    m_warm_mapping_bytes = mapping_bytes;
    m_warm_mapping_addr = (uint8_t *)mapping_addr;
    m_warm_mapped_addr = (uint8_t *)mapped_addr;
    return 0;
}

int vemb_v16_protocol::select_db(int db)
{
    (void)db;
    return 0;  // VEMB does not support SELECT DB
}

int vemb_v16_protocol::authenticate(const char *credentials)
{
    (void)credentials;
    return 0;  // VEMB does not support AUTH
}

int vemb_v16_protocol::configure_protocol(enum PROTOCOL_TYPE type)
{
    (void)type;
    return 0;
}

int vemb_v16_protocol::write_command_cluster_slots()
{
    return 0;  // VEMB does not support CLUSTER SLOTS
}

int vemb_v16_protocol::write_hello(void)
{
    char buf[64];
    ssize_t len = vemb_v16_serialize_hello(buf, sizeof(buf), m_dim, 0);
    if (len < 0) return -1;
    evbuffer_add(m_write_buf, buf, (size_t)len);
    return (int)len;
}

int vemb_v16_protocol::parse_welcome(void)
{
    size_t avail = evbuffer_get_length(m_read_buf);
    if (avail == 0)
        return 0;

    const uint8_t *data = evbuffer_pullup(m_read_buf, -1);
    if (!data)
        return -1;

    vemb_v16_channel_desc_t desc;
    ssize_t consumed = vemb_v16_parse_welcome(data, avail, &desc);
    if (consumed == 0)
        return 0;
    if (consumed < 0)
        return -1;

    evbuffer_drain(m_read_buf, (size_t)consumed);

    m_channel_id = desc.channel_id;
    build_vsim_template();

    if (m_handle_mode && !m_vsim_mode) {
        if (open_warm_region(&desc) != 0) {
            benchmark_error_log("error: failed to open VEMB warm region for handle mode.\n");
            return -1;
        }
    }

    return 1;
}

int vemb_v16_protocol::set_channel_desc(const vemb_v16_channel_desc_t *desc)
{
    if (!desc)
        return -1;

    m_channel_id = desc->channel_id;
    build_vsim_template();
    return 0;
}

int vemb_v16_protocol::build_aeron_set_request(const char *key, int key_len,
                                               const char *value,
                                               int value_len,
                                               int expiry,
                                               unsigned int offset,
                                               vemb_v16_req_t *req,
                                               size_t *req_len)
{
    (void)expiry;
    (void)offset;
    (void)value_len;

    if (!req || !req_len)
        return -1;

    uint32_t actual_key_len = (key_len < (int)VEMB_V16_MAX_KEY_LEN)
        ? (uint32_t)key_len : VEMB_V16_MAX_KEY_LEN - 1;

    memset(req, 0, sizeof(*req));
    req->op = m_vrem_mode ? VEMB_V16_OP_VREM : VEMB_V16_OP_VADD;
    req->flags = m_next_request_flags;
    m_next_request_flags = 0;
    req->req_id = m_req_id++;
    req->channel_id = m_channel_id;
    req->topology_epoch = m_topology_epoch;
    req->key_len = actual_key_len;
    memcpy(req->key, key, actual_key_len);
    req->key_hash = vemb_v16_xxh3_64_str(req->key, req->key_len);

    if (m_vrem_mode) {
        *req_len = offsetof(vemb_v16_req_t, key) + req->key_len;
        return 0;
    }

    req->dim = m_dim;
    req->vector_bytes = m_dim * sizeof(float);
    memcpy(req->vector, value, req->vector_bytes);
    *req_len = offsetof(vemb_v16_req_t, vector) + req->vector_bytes;
    return 0;
}

int vemb_v16_protocol::build_aeron_get_request(const char *key, int key_len,
                                               unsigned int offset,
                                               vemb_v16_req_t *req,
                                               size_t *req_len)
{
    (void)offset;

    if (!req || !req_len || m_vsim_mode)
        return -1;

    uint32_t actual_key_len = (key_len < (int)VEMB_V16_MAX_KEY_LEN)
        ? (uint32_t)key_len : VEMB_V16_MAX_KEY_LEN - 1;

    memset(req, 0, sizeof(*req));
    req->op = m_handle_mode ? VEMB_V16_OP_VEMB_HANDLE : VEMB_V16_OP_VEMB_INLINE;
    req->flags = m_next_request_flags;
    m_next_request_flags = 0;
    req->req_id = m_req_id++;
    req->channel_id = m_channel_id;
    req->topology_epoch = m_topology_epoch;
    req->key_len = actual_key_len;
    req->dim = m_dim;
    req->vector_bytes = m_dim * sizeof(float);
    memcpy(req->key, key, actual_key_len);
    req->key_hash = vemb_v16_xxh3_64_str(req->key, req->key_len);
    *req_len = offsetof(vemb_v16_req_t, key) + req->key_len;
    return 0;
}

int vemb_v16_protocol::write_command_set(const char *key, int key_len,
                                         const char *value, int value_len,
                                         int expiry, unsigned int offset)
{
    (void)expiry;
    (void)offset;
    (void)value_len;

    uint32_t actual_key_len = (key_len < (int)VEMB_V16_MAX_KEY_LEN)
        ? (uint32_t)key_len : VEMB_V16_MAX_KEY_LEN - 1;

    if (m_vrem_mode) {
        uint32_t payload_len = (uint32_t)vemb_v16_req_handle_len();
        size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

        if (evbuffer_expand(m_write_buf, total) != 0)
            return -1;

        struct evbuffer_iovec vec[1];
        if (evbuffer_reserve_space(m_write_buf, total, vec, 1) < 1)
            return -1;

        vemb_v16_req_t req = {0};
        req.op = VEMB_V16_OP_VREM;
        req.flags = m_next_request_flags;
        m_next_request_flags = 0;
        req.req_id = m_req_id++;
        req.channel_id = m_channel_id;
        req.topology_epoch = m_topology_epoch;
        req.key_len = actual_key_len;
        memcpy(req.key, key, actual_key_len);
        req.key_hash = vemb_v16_xxh3_64_str(req.key, req.key_len);

        size_t payload_len_actual = 0;
        if (vemb_v16_req_encode((uint8_t *)vec[0].iov_base + sizeof(vemb_v16_net_hdr_t),
                                vec[0].iov_len - sizeof(vemb_v16_net_hdr_t),
                                &req, &payload_len_actual) != 0)
            return -1;

        vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)vec[0].iov_base;
        memset(hdr, 0, sizeof(*hdr));
        hdr->magic = VEMB_V16_MAGIC;
        hdr->version = VEMB_V16_VERSION;
        hdr->type = VEMB_V16_NET_REQUEST;
        hdr->payload_len = (uint32_t)payload_len_actual;
        hdr->channel_id = m_channel_id;
        hdr->req_id = req.req_id;

        size_t len = sizeof(*hdr) + payload_len_actual;
        vec[0].iov_len = len;
        evbuffer_commit_space(m_write_buf, vec, 1);
        return (int)len;
    }

    uint32_t vector_bytes = m_dim * sizeof(float);
    uint32_t payload_len = (uint32_t)vemb_v16_req_inline_len(vector_bytes);
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

    if (evbuffer_expand(m_write_buf, total) != 0)
        return -1;

    struct evbuffer_iovec vec[1];
    if (evbuffer_reserve_space(m_write_buf, total, vec, 1) < 1)
        return -1;

    vemb_v16_req_t req = {0};
    req.op = VEMB_V16_OP_VADD;
    req.flags = m_next_request_flags;
    m_next_request_flags = 0;
    req.req_id = m_req_id++;
    req.channel_id = m_channel_id;
    req.topology_epoch = m_topology_epoch;
    req.key_len = actual_key_len;
    req.dim = m_dim;
    req.vector_bytes = vector_bytes;
    memcpy(req.key, key, actual_key_len);
    req.key_hash = vemb_v16_xxh3_64_str(req.key, req.key_len);
    memcpy(req.vector, value, vector_bytes);

    size_t payload_len_actual = 0;
    if (vemb_v16_req_encode((uint8_t *)vec[0].iov_base + sizeof(vemb_v16_net_hdr_t),
                            vec[0].iov_len - sizeof(vemb_v16_net_hdr_t),
                            &req, &payload_len_actual) != 0)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)vec[0].iov_base;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len_actual;
    hdr->channel_id = m_channel_id;
    hdr->req_id = req.req_id;

    size_t len = sizeof(*hdr) + payload_len_actual;
    vec[0].iov_len = len;
    evbuffer_commit_space(m_write_buf, vec, 1);
    return (int)len;
}

int vemb_v16_protocol::write_command_get(const char *key, int key_len,
                                         unsigned int offset)
{
    (void)offset;

    uint32_t actual_key_len = (key_len < (int)VEMB_V16_MAX_KEY_LEN)
        ? (uint32_t)key_len : VEMB_V16_MAX_KEY_LEN - 1;

    if (m_vsim_mode && m_vsim_req_template && m_vsim_req_template_size > 0) {
        // ... existing VSIM_INLINE template path ...
        if (evbuffer_expand(m_write_buf, m_vsim_req_template_size) != 0)
            return -1;

        struct evbuffer_iovec vec[1];
        if (evbuffer_reserve_space(m_write_buf, m_vsim_req_template_size, vec, 1) < 1)
            return -1;

        char *p = (char *)vec[0].iov_base;
        memcpy(p, m_vsim_req_template, m_vsim_req_template_size);

        vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)p;
        hdr->req_id = m_req_id++;

        vemb_v16_req_t *req = (vemb_v16_req_t *)(p + sizeof(*hdr));
        req->flags = m_next_request_flags;
        m_next_request_flags = 0;
        req->req_id = hdr->req_id;
        req->topology_epoch = m_topology_epoch;
        req->key_len = actual_key_len;
        memcpy(req->key, key, actual_key_len);
        req->key_hash = vemb_v16_xxh3_64_str(req->key, actual_key_len);

        vec[0].iov_len = m_vsim_req_template_size;
        evbuffer_commit_space(m_write_buf, vec, 1);
        return (int)m_vsim_req_template_size;
    }

    if (m_vsim_key_key_mode) {
        /* VSIM_KEY_KEY: key1 from memtier, key2 random from same prefix */
        /* extract prefix from key1 (e.g. "item:42" → prefix "item:") */
        int prefix_len = actual_key_len;
        const char *ks = key;
        while (prefix_len > 0 && ks[prefix_len-1] >= '0' && ks[prefix_len-1] <= '9')
            prefix_len--;
        if (prefix_len == 0 || prefix_len >= VEMB_V16_MAX_KEY_LEN - 16)
            return -1;

        /* generate random key2 */
        m_key2_rng = m_key2_rng * 6364136223846793005ULL + 1442695040888963407ULL;
        uint64_t key2_num = 1 + (m_key2_rng % 100000);
        char key2_buf[VEMB_V16_MAX_KEY_LEN];
        int key2_len = snprintf(key2_buf, sizeof(key2_buf), "%.*s%llu",
                                prefix_len, ks, (unsigned long long)key2_num);
        if (key2_len <= 0 || key2_len >= VEMB_V16_MAX_KEY_LEN)
            return -1;

        /* build VSIM_KEY_KEY frame */
        vemb_v16_req_t req = {0};
        req.op = VEMB_V16_OP_VSIM_KEY_KEY;
        req.flags = m_next_request_flags;
        m_next_request_flags = 0;
        req.req_id = m_req_id++;
        req.channel_id = m_channel_id;
        req.topology_epoch = m_topology_epoch;
        req.key_len = actual_key_len;
        memcpy(req.key, key, actual_key_len);
        req.key2_len = (uint32_t)key2_len;
        memcpy(req.key2, key2_buf, key2_len);
        req.dim = m_dim;
        req.vector_bytes = m_dim * sizeof(float);
        req.key_hash = vemb_v16_xxh3_64_str(req.key, req.key_len);
        req.key2_hash = vemb_v16_xxh3_64_str(req.key2, req.key2_len);

        size_t payload_len = vemb_v16_req_encoded_len(&req);
        if (payload_len == 0) return -1;

        size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;
        if (evbuffer_expand(m_write_buf, total) != 0)
            return -1;

        struct evbuffer_iovec vec[1];
        if (evbuffer_reserve_space(m_write_buf, total, vec, 1) < 1)
            return -1;

        size_t payload_len_actual = 0;
        if (vemb_v16_req_encode((uint8_t *)vec[0].iov_base + sizeof(vemb_v16_net_hdr_t),
                                vec[0].iov_len - sizeof(vemb_v16_net_hdr_t),
                                &req, &payload_len_actual) != 0)
            return -1;

        vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)vec[0].iov_base;
        memset(hdr, 0, sizeof(*hdr));
        hdr->magic = VEMB_V16_MAGIC;
        hdr->version = VEMB_V16_VERSION;
        hdr->type = VEMB_V16_NET_REQUEST;
        hdr->payload_len = (uint32_t)payload_len_actual;
        hdr->channel_id = m_channel_id;
        hdr->req_id = req.req_id;

        vec[0].iov_len = sizeof(*hdr) + payload_len_actual;
        evbuffer_commit_space(m_write_buf, vec, 1);
        return (int)vec[0].iov_len;
    }

    uint32_t payload_len = (uint32_t)vemb_v16_req_handle_len();
    size_t total = sizeof(vemb_v16_net_hdr_t) + payload_len;

    if (evbuffer_expand(m_write_buf, total) != 0)
        return -1;

    struct evbuffer_iovec vec[1];
    if (evbuffer_reserve_space(m_write_buf, total, vec, 1) < 1)
        return -1;

    vemb_v16_req_t req = {0};
    req.op = m_handle_mode ? VEMB_V16_OP_VEMB_HANDLE : VEMB_V16_OP_VEMB_INLINE;
    req.flags = m_next_request_flags;
    m_next_request_flags = 0;
    req.req_id = m_req_id++;
    req.channel_id = m_channel_id;
    req.topology_epoch = m_topology_epoch;
    req.key_len = actual_key_len;
    req.dim = m_dim;
    req.vector_bytes = m_dim * sizeof(float);
    memcpy(req.key, key, actual_key_len);
    req.key_hash = vemb_v16_xxh3_64_str(req.key, req.key_len);

    size_t payload_len_actual = 0;
    if (vemb_v16_req_encode((uint8_t *)vec[0].iov_base + sizeof(vemb_v16_net_hdr_t),
                            vec[0].iov_len - sizeof(vemb_v16_net_hdr_t),
                            &req, &payload_len_actual) != 0)
        return -1;

    vemb_v16_net_hdr_t *hdr = (vemb_v16_net_hdr_t *)vec[0].iov_base;
    memset(hdr, 0, sizeof(*hdr));
    hdr->magic = VEMB_V16_MAGIC;
    hdr->version = VEMB_V16_VERSION;
    hdr->type = VEMB_V16_NET_REQUEST;
    hdr->payload_len = (uint32_t)payload_len_actual;
    hdr->channel_id = m_channel_id;
    hdr->req_id = req.req_id;

    size_t len = sizeof(*hdr) + payload_len_actual;
    vec[0].iov_len = len;
    evbuffer_commit_space(m_write_buf, vec, 1);
    return (int)len;
}

int vemb_v16_protocol::write_command_multi_get(const keylist *keylist)
{
    (void)keylist;
    assert(0);
    return -1;
}

int vemb_v16_protocol::write_command_wait(unsigned int num_slaves,
                                           unsigned int timeout)
{
    (void)num_slaves;
    (void)timeout;
    return 0;
}

int vemb_v16_protocol::parse_response()
{
    m_last_response.clear();

    size_t avail = evbuffer_get_length(m_read_buf);
    if (avail == 0)
        return 0;

    const uint8_t *data = evbuffer_pullup(m_read_buf, -1);
    if (!data)
        return -1;

    vemb_v16_resp_t resp;
    size_t inline_bytes = 0;
    ssize_t consumed = vemb_v16_parse_response(data, avail, &resp, &inline_bytes);
    if (consumed == 0)
        return 0;
    if (consumed < 0)
        return -1;

    if (resp.status == VEMB_V16_STATUS_OK) {
        m_last_response.set_status(strdup("OK"));
        m_last_response.incr_hits();
    } else if (resp.status == VEMB_V16_STATUS_NOT_FOUND) {
        m_last_response.set_status(strdup("NOT_FOUND"));
        // miss: no hit count
    } else {
        const char *status = "ERR";
        if (resp.status == VEMB_V16_STATUS_STALE_TOPOLOGY) {
            status = "STALE_TOPOLOGY";
        } else if (resp.status == VEMB_V16_STATUS_MOVED) {
            status = "MOVED";
        } else if (resp.status == VEMB_V16_STATUS_ASK) {
            status = "ASK";
        }
        m_last_response.set_status(strdup(status));
        m_last_response.set_error();
    }
    m_last_response.set_vemb_v16_status(resp.status, resp.redirect_owner);

    if (m_keep_value && resp.status == VEMB_V16_STATUS_OK &&
        (resp.op == VEMB_V16_OP_VEMB_HANDLE || resp.op == VEMB_V16_OP_VEMB_INLINE) &&
        resp.vector_bytes > 0) {
        char *value = NULL;
        if (resp.op == VEMB_V16_OP_VEMB_INLINE) {
            if (inline_bytes != resp.vector_bytes) {
                return -1;
            }
            value = (char *)malloc(resp.vector_bytes);
            if (!value)
                return -1;
            sve_streaming_load_f32(data + consumed - inline_bytes,
                                   value,
                                   resp.vector_bytes);
        } else if (vemb_v16_copy_handle_vector(&resp,
                                               m_warm_mapped_addr,
                                               m_warm_region_bytes,
                                               &value) != 0) {
            return -1;
        }
        m_last_response.set_value(value, resp.vector_bytes);
    }

    evbuffer_drain(m_read_buf, (size_t)consumed);
    m_last_response.set_total_len((unsigned int)consumed);
    return 1;
}

int vemb_v16_protocol::parse_aeron_response(const vemb_v16_resp_t *resp,
                                            const uint8_t *inline_data,
                                            uint32_t inline_bytes,
                                            const uint8_t *warm_mapped_addr,
                                            uint64_t warm_region_bytes)
{
    if (!resp)
        return -1;

    m_last_response.clear();

    if (resp->status == VEMB_V16_STATUS_OK) {
        m_last_response.set_status(strdup("OK"));
        m_last_response.incr_hits();
    } else if (resp->status == VEMB_V16_STATUS_NOT_FOUND) {
        m_last_response.set_status(strdup("NOT_FOUND"));
    } else {
        const char *status = "ERR";
        if (resp->status == VEMB_V16_STATUS_STALE_TOPOLOGY) {
            status = "STALE_TOPOLOGY";
        } else if (resp->status == VEMB_V16_STATUS_MOVED) {
            status = "MOVED";
        } else if (resp->status == VEMB_V16_STATUS_ASK) {
            status = "ASK";
        }
        m_last_response.set_status(strdup(status));
        m_last_response.set_error();
    }
    m_last_response.set_vemb_v16_status(resp->status, resp->redirect_owner);

    if (m_keep_value && resp->status == VEMB_V16_STATUS_OK &&
        (resp->op == VEMB_V16_OP_VEMB_HANDLE || resp->op == VEMB_V16_OP_VEMB_INLINE) &&
        resp->vector_bytes > 0) {
        char *value = NULL;
        if (resp->op == VEMB_V16_OP_VEMB_INLINE) {
            if (!inline_data || inline_bytes != resp->vector_bytes)
                return -1;
            value = (char *)malloc(resp->vector_bytes);
            if (!value)
                return -1;
            sve_streaming_load_f32(inline_data, value, resp->vector_bytes);
        } else if (vemb_v16_copy_handle_vector(resp,
                                               warm_mapped_addr,
                                               warm_region_bytes,
                                               &value) != 0) {
            return -1;
        }
        m_last_response.set_value(value, resp->vector_bytes);
    }

    m_last_response.set_total_len((unsigned int)sizeof(*resp) + inline_bytes);
    return 1;
}

bool vemb_v16_protocol::format_arbitrary_command(arbitrary_command &cmd)
{
    (void)cmd;
    return false;
}

int vemb_v16_protocol::write_arbitrary_command(const command_arg *arg)
{
    (void)arg;
    assert(0);
    return -1;
}

int vemb_v16_protocol::write_arbitrary_command(const char *val, int val_len)
{
    (void)val;
    (void)val_len;
    assert(0);
    return -1;
}

class abstract_protocol *protocol_factory(enum PROTOCOL_TYPE type)
{
    if (is_redis_protocol(type)) {
        return new redis_protocol();
    } else if (type == PROTOCOL_MEMCACHE_TEXT) {
        return new memcache_text_protocol();
    } else if (type == PROTOCOL_MEMCACHE_BINARY) {
        return new memcache_binary_protocol();
    } else if (type == PROTOCOL_VEMB_V16) {
        return new vemb_v16_protocol();
    } else {
        benchmark_error_log("Error: unknown protocol type: %d.\n", type);
        return NULL;
    }
}

/////////////////////////////////////////////////////////////////////////

keylist::keylist(unsigned int max_keys) :
    m_buffer(NULL), m_buffer_ptr(NULL), m_buffer_size(0),
    m_keys(NULL), m_keys_size(0), m_keys_count(0)
{
    m_keys_size = max_keys;
    m_keys = (key_entry *) malloc(m_keys_size * sizeof(key_entry));
    assert(m_keys != NULL);
    memset(m_keys, 0, m_keys_size * sizeof(key_entry));

    /* allocate buffer for actual keys */
    m_buffer_size = 256 * m_keys_size;
    m_buffer = (char *) malloc(m_buffer_size);
    assert(m_buffer != NULL);
    memset(m_buffer, 0, m_buffer_size);

    m_buffer_ptr = m_buffer;
}

keylist::~keylist()
{
    if (m_buffer != NULL) {
        free(m_buffer);
        m_buffer = NULL;
    }
    if (m_keys != NULL) {
        free(m_keys);
        m_keys = NULL;
    }
}

bool keylist::add_key(const char *key, unsigned int key_len)
{
    // have room?
    if (m_keys_count >= m_keys_size)
        return false;

    // have buffer?
    if (m_buffer_ptr + key_len >= m_buffer + m_buffer_size) {
        while (m_buffer_ptr + key_len >= m_buffer + m_buffer_size) {
            m_buffer_size *= 2;
        }
        m_buffer = (char *)realloc(m_buffer, m_buffer_size);
        assert(m_buffer != NULL);
    }

    // copy key
    memcpy(m_buffer_ptr, key, key_len);
    m_buffer_ptr[key_len] = '\0';
    m_keys[m_keys_count].key_ptr = m_buffer_ptr;
    m_keys[m_keys_count].key_len = key_len;

    m_buffer_ptr += key_len + 1;
    m_keys_count++;

    return true;
}

unsigned int keylist::get_keys_count(void) const
{
    return m_keys_count;
}

const char *keylist::get_key(unsigned int index, unsigned int *key_len) const
{
    if (index < 0 || index >= m_keys_count)
        return NULL;
    if (key_len != NULL)
        *key_len = m_keys[index].key_len;
    return m_keys[index].key_ptr;
}

void keylist::clear(void)
{
    m_keys_count = 0;
    m_buffer_ptr = m_buffer;
}
