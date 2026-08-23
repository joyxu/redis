# Redis Makefile
# Copyright (c) 2011-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of (a) the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
#
# The Makefile composes the final FINAL_CFLAGS and FINAL_LDFLAGS using
# what is needed for Redis plus the standard CFLAGS and LDFLAGS passed.
# However when building the dependencies (Jemalloc, Lua, Hiredis, ...)
# CFLAGS and LDFLAGS are propagated to the dependencies, so to pass
# flags only to be used when compiling / linking Redis itself REDIS_CFLAGS
# and REDIS_LDFLAGS are used instead (this is the case of 'make gcov').
#
# Dependencies are stored in the Makefile.dep file. To rebuild this file
# Just use 'make dep', but this is only needed by developers.

release_hdr := $(shell sh -c './mkreleasehdr.sh')
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
uname_M := $(shell sh -c 'uname -m 2>/dev/null || echo not')
CLANG := $(findstring clang,$(shell sh -c '$(CC) --version | head -1'))

# Build output directory
OUT_DIR := ../output/src

# Optimization flags. To override, the OPTIMIZATION variable can be passed, but
# some automatic defaults are added to it. To specify optimization flags
# explicitly without any defaults added, pass the OPT variable instead.
OPTIMIZATION?=-O3
ifeq ($(OPTIMIZATION),-O3)
	ifeq (clang,$(CLANG))
		OPTIMIZATION+=-flto
	else
		OPTIMIZATION+=-flto=auto
	endif
endif
ifneq ($(OPTIMIZATION),-O0)
	OPTIMIZATION+=-fno-omit-frame-pointer
endif
DEPENDENCY_TARGETS=hiredis linenoise lua hdr_histogram fpconv fast_float xxhash
NODEPS:=clean distclean

# Default settings
STD=-pedantic -DREDIS_STATIC=''

# Use -Wno-c11-extensions on clang, either where explicitly used or on
# platforms we can assume it's being used.
ifeq (clang,$(CLANG))
  STD+=-Wno-c11-extensions
else
ifneq (,$(findstring FreeBSD,$(uname_S)))
  STD+=-Wno-c11-extensions
endif
endif
WARN=-Wall -W -Wno-missing-field-initializers -Werror=deprecated-declarations -Wstrict-prototypes
OPT=$(OPTIMIZATION)

SKIP_VEC_SETS?=no
PROXY_REQUEST_BATCH ?=
PROXY_RESPONSE_BATCH ?=
PROXY_QUEUE_BATCH ?=
VEMB_V16_PROXY_AFFINITY_MODE ?= 0
# Detect if the compiler supports C11 _Atomic.
# NUMBER_SIGN_CHAR is a workaround to support both GNU Make 4.3 and older versions.
NUMBER_SIGN_CHAR := \#
C11_ATOMIC := $(shell sh -c 'echo "$(NUMBER_SIGN_CHAR)include <stdatomic.h>" > foo.c; \
	$(CC) -std=gnu11 -c foo.c -o foo.o > /dev/null 2>&1; \
	if [ -f foo.o ]; then echo "yes"; rm foo.o; fi; rm foo.c')
ifeq ($(C11_ATOMIC),yes)
	STD+=-std=gnu11
else
	SKIP_VEC_SETS=yes
	STD+=-std=c99
endif

PREFIX?=/usr/local
INSTALL_BIN=$(PREFIX)/bin
INSTALL=install
PKG_CONFIG?=pkg-config

ifndef PYTHON
PYTHON := $(shell which python3 || which python)
endif

# Default allocator defaults to Jemalloc on Linux and libc otherwise
MALLOC=libc
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
endif

# To get ARM stack traces if Redis crashes we need a special C flag.
ifneq (,$(filter aarch64 armv%,$(uname_M)))
        CFLAGS+=-funwind-tables
endif

# Backwards compatibility for selecting an allocator
ifeq ($(USE_TCMALLOC),yes)
	MALLOC=tcmalloc
endif

ifeq ($(USE_TCMALLOC_MINIMAL),yes)
	MALLOC=tcmalloc_minimal
endif

ifeq ($(USE_JEMALLOC),yes)
	MALLOC=jemalloc
endif

ifeq ($(USE_JEMALLOC),no)
	MALLOC=libc
endif

ifdef SANITIZER
ifeq ($(SANITIZER),address)
	MALLOC=libc
	CFLAGS+=-fsanitize=address -fno-sanitize-recover=all -fno-omit-frame-pointer
	LDFLAGS+=-fsanitize=address
else
ifeq ($(SANITIZER),undefined)
	MALLOC=libc
	CFLAGS+=-fsanitize=undefined -fno-sanitize-recover=all -fno-omit-frame-pointer
	LDFLAGS+=-fsanitize=undefined
else
ifeq ($(SANITIZER),thread)
	CFLAGS+=-fsanitize=thread -fno-sanitize-recover=all -fno-omit-frame-pointer
	LDFLAGS+=-fsanitize=thread
else
ifeq ($(SANITIZER),memory)
ifeq (clang, $(CLANG))
	export CXX:=clang
	export LD:=clang
	MALLOC=libc # MSan provides its own allocator so make sure not to use jemalloc as they clash
	CFLAGS+=-fsanitize=memory -fsanitize-memory-track-origins=2 -fno-sanitize-recover=all -fno-omit-frame-pointer
	LDFLAGS+=-fsanitize=memory
else
    $(error "MemorySanitizer needs to be compiled and linked with clang. Please use CC=clang")
endif
else
    $(error "unknown sanitizer=${SANITIZER}")
endif
endif
endif
endif
endif

# Special case of forcing defrag to run even though we have no Jemlloc support
ifeq ($(DEBUG_DEFRAG), force)
	CFLAGS +=-DDEBUG_DEFRAG_FORCE
else ifeq ($(DEBUG_DEFRAG), fully)
	CFLAGS +=-DDEBUG_DEFRAG_FORCE -DDEBUG_DEFRAG_FULLY
endif

# Override default settings if possible
-include .make-settings

# ============================================================
# HPC Feature Flags (build_opts/*.mk)
# ============================================================
# Initialize aggregation variables
FEATURE_MARCH_SUFFIX :=
FEATURE_CFLAGS  :=
FEATURE_LDFLAGS :=
FEATURE_LIBS    :=
FEATURE_OBJS    :=

# Load all feature fragment files
include build_opts/*.mk

FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS) $(REDIS_CFLAGS)
FINAL_LDFLAGS=$(LDFLAGS) $(OPT) $(REDIS_LDFLAGS) $(DEBUG)
FINAL_LIBS=-lm -lstdc++

# Apply aggregated feature flags directly to FINAL_* (not CFLAGS/REDIS_CFLAGS)
# This prevents persist-settings from saving feature flags into CFLAGS/REDIS_CFLAGS
# and causing duplication on subsequent builds.
empty :=
space := $(empty) $(empty)
ifneq ($(strip $(FEATURE_MARCH_SUFFIX)),)
    FINAL_CFLAGS += -march=armv8.2-a$(subst $(space),,$(strip $(FEATURE_MARCH_SUFFIX)))
endif
FINAL_CFLAGS  += $(FEATURE_CFLAGS)
FINAL_LDFLAGS += $(FEATURE_LDFLAGS)
FINAL_LIBS    += $(FEATURE_LIBS)
# Enable ARM SVE intrinsics for vector load/store paths (sve_operation.c, tlc_core.c)
FINAL_CFLAGS += -DUSE_ARM_SVE -march=armv8.2-a+sve
ifneq ($(strip $(PROXY_REQUEST_BATCH)),)
FINAL_CFLAGS += -DPROXY_REQUEST_BATCH=$(PROXY_REQUEST_BATCH)
endif
ifneq ($(strip $(PROXY_RESPONSE_BATCH)),)
FINAL_CFLAGS += -DPROXY_RESPONSE_BATCH=$(PROXY_RESPONSE_BATCH)
endif
ifneq ($(strip $(PROXY_QUEUE_BATCH)),)
FINAL_CFLAGS += -DPROXY_QUEUE_BATCH=$(PROXY_QUEUE_BATCH)
endif
FINAL_CFLAGS += -DVEMB_V16_PROXY_AFFINITY_MODE=$(VEMB_V16_PROXY_AFFINITY_MODE)
DEBUG=-g -ggdb

# Linux ARM32 needs -latomic at linking time
ifneq (,$(findstring armv,$(uname_M)))
        FINAL_LIBS+=-latomic
endif

ifeq ($(uname_S),SunOS)
	# SunOS
	ifeq ($(findstring -m32,$(FINAL_CFLAGS)),)
		CFLAGS+=-m64
	endif
	ifeq ($(findstring -m32,$(FINAL_LDFLAGS)),)
		LDFLAGS+=-m64
	endif
	DEBUG=-g
	DEBUG_FLAGS=-g
	export CFLAGS LDFLAGS DEBUG DEBUG_FLAGS
	INSTALL=cp -pf
	FINAL_CFLAGS+= -D__EXTENSIONS__ -D_XPG6
	FINAL_LIBS+= -ldl -lnsl -lsocket -lresolv -lpthread -lrt
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE
	endif
else
ifeq ($(uname_S),Darwin)
	# Darwin
	FINAL_LIBS+= -ldl
	# Homebrew's OpenSSL is not linked to /usr/local to avoid
	# conflicts with the system's LibreSSL installation so it
	# must be referenced explicitly during build.
ifeq ($(uname_M),arm64)
	# Homebrew arm64 uses /opt/homebrew as HOMEBREW_PREFIX
	OPENSSL_PREFIX?=/opt/homebrew/opt/openssl
else
	# Homebrew x86/ppc uses /usr/local as HOMEBREW_PREFIX
	OPENSSL_PREFIX?=/usr/local/opt/openssl
endif
else
ifeq ($(uname_S),AIX)
        # AIX
        FINAL_LDFLAGS+= -Wl,-bexpall
        FINAL_LIBS+=-ldl -pthread -lcrypt -lbsd
else
ifeq ($(uname_S),OpenBSD)
	# OpenBSD
	FINAL_LIBS+= -lpthread
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE -I/usr/local/include
	    FINAL_LDFLAGS+= -L/usr/local/lib
	    FINAL_LIBS+= -lexecinfo
    	endif

else
ifeq ($(uname_S),NetBSD)
	# NetBSD
	FINAL_LIBS+= -lpthread
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE -I/usr/pkg/include
	    FINAL_LDFLAGS+= -L/usr/pkg/lib
	    FINAL_LIBS+= -lexecinfo
    	endif
else
ifeq ($(uname_S),FreeBSD)
	# FreeBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),DragonFly)
	# DragonFly
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),OpenBSD)
	# OpenBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),NetBSD)
	# NetBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),Haiku)
	# Haiku
	FINAL_CFLAGS+= -DBSD_SOURCE
	FINAL_LDFLAGS+= -lbsd -lnetwork
	FINAL_LIBS+= -lpthread
else
	# All the other OSes (notably Linux)
	FINAL_LDFLAGS+= -rdynamic
	FINAL_LIBS+=-ldl -pthread -lrt
endif
endif
endif
endif
endif
endif
endif
endif
endif
endif

ifdef OPENSSL_PREFIX
	OPENSSL_CFLAGS=-I$(OPENSSL_PREFIX)/include
	OPENSSL_LDFLAGS=-L$(OPENSSL_PREFIX)/lib
	# Also export OPENSSL_PREFIX so it ends up in deps sub-Makefiles
	export OPENSSL_PREFIX
endif

# Include paths to dependencies
FINAL_CFLAGS+= -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src -I../deps/hdr_histogram -I../deps/fpconv -I../deps/fast_float -I../deps/xxhash

	# Dependency library paths (redirected to output/deps/)
	DEPS_OUT_DIR := ../output/deps
	DEP_LIB_HIREDIS := $(DEPS_OUT_DIR)/hiredis/libhiredis.a
	DEP_LIB_LUA := $(DEPS_OUT_DIR)/lua/liblua.a
	DEP_LIB_HDR := $(DEPS_OUT_DIR)/hdr_histogram/libhdrhistogram.a
	DEP_LIB_FPCONV := $(DEPS_OUT_DIR)/fpconv/libfpconv.a
	DEP_LIB_FAST_FLOAT := $(DEPS_OUT_DIR)/fast_float/libfast_float.a
	DEP_LIB_XXHASH := $(DEPS_OUT_DIR)/xxhash/libxxhash.a
	DEP_OBJ_LINENOISE := $(DEPS_OUT_DIR)/linenoise/linenoise.o
	ifeq ($(MALLOC),jemalloc)
		DEP_LIB_JEMALLOC := ../deps/jemalloc/lib/libjemalloc.a
	else
		DEP_LIB_JEMALLOC :=
	endif

# Determine systemd support and/or build preference (defaulting to auto-detection)
BUILD_WITH_SYSTEMD=no
LIBSYSTEMD_LIBS=-lsystemd

# If 'USE_SYSTEMD' in the environment is neither "no" nor "yes", try to
# auto-detect libsystemd's presence and link accordingly.
ifneq ($(USE_SYSTEMD),no)
	LIBSYSTEMD_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libsystemd && echo $$?)
# If libsystemd cannot be detected, continue building without support for it
# (unless a later check tells us otherwise)
ifeq ($(LIBSYSTEMD_PKGCONFIG),0)
	BUILD_WITH_SYSTEMD=yes
	LIBSYSTEMD_LIBS=$(shell $(PKG_CONFIG) --libs libsystemd)
endif
endif

# If 'USE_SYSTEMD' is set to "yes" use pkg-config if available or fall back to
# default -lsystemd.
ifeq ($(USE_SYSTEMD),yes)
	BUILD_WITH_SYSTEMD=yes
endif

ifeq ($(BUILD_WITH_SYSTEMD),yes)
	FINAL_LIBS+=$(LIBSYSTEMD_LIBS)
	FINAL_CFLAGS+= -DHAVE_LIBSYSTEMD
endif

ifeq ($(MALLOC),tcmalloc)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc
endif

ifeq ($(MALLOC),tcmalloc_minimal)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc_minimal
endif

ifeq ($(MALLOC),jemalloc)
	DEPENDENCY_TARGETS+= jemalloc
	FINAL_CFLAGS+= -DUSE_JEMALLOC -I../deps/jemalloc/include
	FINAL_LIBS := $(DEP_LIB_JEMALLOC) $(FINAL_LIBS)
endif

# LIBSSL & LIBCRYPTO
LIBSSL_LIBS=
LIBSSL_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libssl && echo $$?)
ifeq ($(LIBSSL_PKGCONFIG),0)
	LIBSSL_LIBS=$(shell $(PKG_CONFIG) --libs libssl)
else
	LIBSSL_LIBS=-lssl
endif
LIBCRYPTO_LIBS=
LIBCRYPTO_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libcrypto && echo $$?)
ifeq ($(LIBCRYPTO_PKGCONFIG),0)
	LIBCRYPTO_LIBS=$(shell $(PKG_CONFIG) --libs libcrypto)
else
	LIBCRYPTO_LIBS=-lcrypto
endif

BUILD_NO:=0
BUILD_YES:=1
BUILD_MODULE:=2
ifeq ($(BUILD_TLS),yes)
	FINAL_CFLAGS+=-DUSE_OPENSSL=$(BUILD_YES) $(OPENSSL_CFLAGS) -DBUILD_TLS_MODULE=$(BUILD_NO)
	FINAL_LDFLAGS+=$(OPENSSL_LDFLAGS)
	FINAL_LIBS += $(DEPS_OUT_DIR)/hiredis/libhiredis_ssl.a $(LIBSSL_LIBS) $(LIBCRYPTO_LIBS)
endif

TLS_MODULE=
TLS_MODULE_NAME:=redis-tls$(PROG_SUFFIX).so
TLS_MODULE_CFLAGS:=$(FINAL_CFLAGS)
ifeq ($(BUILD_TLS),module)
	FINAL_CFLAGS+=-DUSE_OPENSSL=$(BUILD_MODULE) $(OPENSSL_CFLAGS)
	TLS_CLIENT_LIBS = $(DEPS_OUT_DIR)/hiredis/libhiredis_ssl.a $(LIBSSL_LIBS) $(LIBCRYPTO_LIBS)
	TLS_MODULE=$(TLS_MODULE_NAME)
	TLS_MODULE_CFLAGS+=-DUSE_OPENSSL=$(BUILD_MODULE) $(OPENSSL_CFLAGS) -DBUILD_TLS_MODULE=$(BUILD_MODULE)
endif

ifneq ($(SKIP_VEC_SETS),yes)
	vpath %.c ../modules/vector-sets
	REDIS_VEC_SETS_OBJ=hnsw.o vset.o vset_config.o
	FINAL_CFLAGS+=-DINCLUDE_VEC_SETS=1
endif

ifndef V
    define MAKE_INSTALL
        @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$(1)$(ENDCOLOR) 1>&2
        @$(INSTALL) $(1) $(2)
    endef
else
    define MAKE_INSTALL
        $(INSTALL) $(1) $(2)
    endef
endif

REDIS_CC=$(QUIET_CC)$(CC) $(FINAL_CFLAGS)
REDIS_LD=$(QUIET_LINK)$(CC) $(FINAL_LDFLAGS)
REDIS_INSTALL=$(QUIET_INSTALL)$(INSTALL)

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

ifndef V
QUIET_CC = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_GEN = @printf '    %b %b\n' $(CCCOLOR)GEN$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
QUIET_INSTALL = @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2;
endif

ifneq (, $(findstring LOG_REQ_RES, $(REDIS_CFLAGS)))
	COMMANDS_DEF_FILENAME=commands_with_reply_schema
	GEN_COMMANDS_FLAGS=--with-reply-schema
else
	COMMANDS_DEF_FILENAME=commands
	GEN_COMMANDS_FLAGS=
endif

REDIS_SERVER_NAME=redis-server$(PROG_SUFFIX)
REDIS_SENTINEL_NAME=redis-sentinel$(PROG_SUFFIX)
REDIS_SERVER_OBJ=threads_mngr.o memory_prefetch.o adlist.o quicklist.o ae.o anet.o dict.o ebuckets.o eventnotifier.o iothread.o mstr.o entry.o kvstore.o fwtree.o estore.o server.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o ziplist.o release.o networking.o util.o object.o db.o replication.o rdb.o t_string.o t_list.o t_set.o t_zset.o t_hash.o config.o aof.o pubsub.o multi.o debug.o sort.o intset.o syncio.o cluster.o cluster_asm.o cluster_legacy.o cluster_slot_stats.o crc16.o endianconv.o slowlog.o eval.o bio.o rio.o rand.o memtest.o syscheck.o crcspeed.o crccombine.o crc64.o bitops.o sentinel.o notify.o setproctitle.o blocked.o hyperloglog.o latency.o sparkline.o redis-check-rdb.o redis-check-aof.o geo.o lazyfree.o module.o evict.o expire.o geohash.o geohash_helper.o childinfo.o defrag.o siphash.o rax.o t_stream.o listpack.o localtime.o lolwut.o lolwut5.o lolwut6.o lolwut8.o acl.o tracking.o socket.o tls.o sha256.o timeout.o setcpuaffinity.o monotonic.o mt19937-64.o resp_parser.o call_reply.o script_lua.o script.o functions.o function_lua.o commands.o strl.o connection.o unix.o logreqres.o keymeta.o chk.o hotkeys.o vector_engine.o vector_engine_redis_impl.o vector_engine_ub_impl.o ub_client.o ub_metadata.o sve_compute.o sve_similarity.o batch_processor.o vector_proxy_request.o vector_proxy_completion.o batch_latency_trace.o proxy_aggregator.o supernode_worker.o ring_buffer_mgr.o ring_buffer.o sve_operation.o \
                    vemb_v16_server_integration.o vemb_v16_server_tcp_client.o \
                    futex.o vemb_v16_proxy.o vemb_v16_supernode.o vemb_v16_tlc.o tlc_core.o \
                    vemb_v16_net.o vemb_v16_storage.o vemb_v16_warm_provider.o vemb_v16_tcp_transport.o \
                    vemb_v16_aeron_transport.o vemb_v16_aeron_attach.o vemb_v16_stats.o \
                    vemb_v16_remote_meta.o vemb_v16_ub_rpc.o vemb_v16_topology.o \
                    vemb_v16_migration_outbox.o \
                    vemb_v16_shared_allocator.o vemb_v16_mapped_region.o \
                    vemb_v16_warm_region_layout.o \
                    vemb_v16_control_listener.o
REDIS_SERVER_OBJ += $(FEATURE_OBJS)
REDIS_CLI_NAME=$(OUT_DIR)/redis-cli$(PROG_SUFFIX)
REDIS_CLI_OBJ=anet.o adlist.o dict.o redis-cli.o zmalloc.o release.o ae.o redisassert.o crcspeed.o crccombine.o crc64.o siphash.o crc16.o monotonic.o cli_common.o mt19937-64.o strl.o cli_commands.o vemb_v16_cli_tcp.o vemb_v16_client_sdk.o vemb_v16_cli_l0.o vemb_v16_cli_l1.o vemb_v16_cluster_core.o vemb_v16_owner_session.o vemb_v16_ub_peer_view.o vemb_v16_net.o vemb_v16_client_topology.o vemb_v16_topology.o
REDIS_BENCHMARK_NAME=$(OUT_DIR)/redis-benchmark$(PROG_SUFFIX)
REDIS_BENCHMARK_OBJ=ae.o anet.o redis-benchmark.o adlist.o dict.o zmalloc.o redisassert.o release.o crcspeed.o crccombine.o crc64.o siphash.o crc16.o monotonic.o cli_common.o mt19937-64.o strl.o
# HPC-Redis: link the VEMB V16 client SDK as a static archive (per CLAUDE.md
# constraint — do NOT copy SDK .c files into src/).
REDIS_BENCHMARK_VEMB_LIB=../clients/c/build/libvemb_v16_client.a
REDIS_BENCHMARK_VEMB_INC=-I../clients/c/build/include -I../src

# Auto-build the client SDK static archive (and flat headers) whenever
# redis-benchmark needs it. The .a lands in clients/c/build/ and the public
# headers in clients/c/build/include/.
$(REDIS_BENCHMARK_VEMB_LIB):
	$(MAKE) -C ../clients/c static install-headers
REDIS_CHECK_RDB_NAME=$(OUT_DIR)/redis-check-rdb$(PROG_SUFFIX)
REDIS_CHECK_AOF_NAME=$(OUT_DIR)/redis-check-aof$(PROG_SUFFIX)
ALL_SOURCES=$(sort $(patsubst %.o,%.c,$(REDIS_SERVER_OBJ) $(REDIS_VEC_SETS_OBJ) $(REDIS_CLI_OBJ) $(REDIS_BENCHMARK_OBJ)))

# Output-prefixed object lists for linking
REDIS_SERVER_OBJ_OUT=$(addprefix $(OUT_DIR)/,$(REDIS_SERVER_OBJ))
REDIS_VEC_SETS_OBJ_OUT=$(addprefix $(OUT_DIR)/,$(REDIS_VEC_SETS_OBJ))
REDIS_CLI_OBJ_OUT=$(addprefix $(OUT_DIR)/,$(REDIS_CLI_OBJ))
REDIS_BENCHMARK_OBJ_OUT=$(addprefix $(OUT_DIR)/,$(REDIS_BENCHMARK_OBJ))

all: $(REDIS_SERVER_NAME) $(REDIS_SENTINEL_NAME) $(REDIS_CLI_NAME) $(REDIS_BENCHMARK_NAME) $(REDIS_CHECK_RDB_NAME) $(REDIS_CHECK_AOF_NAME) $(TLS_MODULE) module_tests
	@echo ""
	@echo "Hint: It's a good idea to run 'make test' ;)"
	@echo ""

Makefile.dep:
	-$(REDIS_CC) -MM $(ALL_SOURCES) > Makefile.dep 2> /dev/null || true

ifeq (0, $(words $(findstring $(MAKECMDGOALS), $(NODEPS))))
-include Makefile.dep
endif

.PHONY: all

module_tests: $(REDIS_SERVER_NAME)
	$(MAKE) -C ../tests/modules

.PHONY: module_tests

persist-settings: distclean
	echo STD=$(STD) >> .make-settings
	echo WARN=$(WARN) >> .make-settings
	echo OPT=$(OPT) >> .make-settings
	echo MALLOC=$(MALLOC) >> .make-settings
	echo BUILD_TLS=$(BUILD_TLS) >> .make-settings
	echo USE_SYSTEMD=$(USE_SYSTEMD) >> .make-settings
	echo CFLAGS=$(CFLAGS) >> .make-settings
	echo LDFLAGS=$(LDFLAGS) >> .make-settings
	echo REDIS_CFLAGS=$(REDIS_CFLAGS) >> .make-settings
	echo REDIS_LDFLAGS=$(REDIS_LDFLAGS) >> .make-settings
	echo PREV_FINAL_CFLAGS=$(FINAL_CFLAGS) >> .make-settings
	echo PREV_FINAL_LDFLAGS=$(FINAL_LDFLAGS) >> .make-settings
	echo USE_SVE2=$(USE_SVE2) >> .make-settings
	echo USE_SVE=$(USE_SVE) >> .make-settings
	echo USE_LSE=$(USE_LSE) >> .make-settings
	echo USE_F32MM=$(USE_F32MM) >> .make-settings
	echo USE_NPU=$(USE_NPU) >> .make-settings
	echo USE_UB=$(USE_UB) >> .make-settings
	echo USE_CC_MODE=$(USE_CC_MODE) >> .make-settings
	echo USE_TLC=$(USE_TLC) >> .make-settings
	-(cd ../deps && $(MAKE) $(DEPENDENCY_TARGETS))

.PHONY: persist-settings

# Prerequisites target
.make-prerequisites:
	@touch $@

# Clean everything, persist settings and build dependencies if anything changed
ifneq ($(strip $(PREV_FINAL_CFLAGS)), $(strip $(FINAL_CFLAGS)))
.make-prerequisites: persist-settings
endif

ifneq ($(strip $(PREV_FINAL_LDFLAGS)), $(strip $(FINAL_LDFLAGS)))
.make-prerequisites: persist-settings
endif

# redis-server
$(REDIS_SERVER_NAME): $(REDIS_SERVER_OBJ_OUT) $(REDIS_VEC_SETS_OBJ_OUT)
	@mkdir -p $(OUT_DIR)
	$(REDIS_LD) -o $@ $^ $(DEP_LIB_HIREDIS) $(DEP_LIB_LUA) $(DEP_LIB_HDR) $(DEP_LIB_FPCONV) $(DEP_LIB_FAST_FLOAT) $(DEP_LIB_XXHASH) $(FINAL_LIBS)

# redis-sentinel
$(REDIS_SENTINEL_NAME): $(REDIS_SERVER_NAME)
	$(REDIS_INSTALL) $(REDIS_SERVER_NAME) $(REDIS_SENTINEL_NAME)

# redis-check-rdb
$(REDIS_CHECK_RDB_NAME): $(REDIS_SERVER_NAME)
	$(REDIS_INSTALL) $(REDIS_SERVER_NAME) $(REDIS_CHECK_RDB_NAME)

# redis-check-aof
$(REDIS_CHECK_AOF_NAME): $(REDIS_SERVER_NAME)
	$(REDIS_INSTALL) $(REDIS_SERVER_NAME) $(REDIS_CHECK_AOF_NAME)

# redis-tls.so
$(TLS_MODULE_NAME): $(REDIS_SERVER_NAME)
	@mkdir -p $(OUT_DIR)
	$(QUIET_CC)$(CC) -o $(OUT_DIR)/$@ tls.c -shared -fPIC $(TLS_MODULE_CFLAGS) $(TLS_CLIENT_LIBS)

# redis-cli
$(REDIS_CLI_NAME): $(REDIS_CLI_OBJ_OUT)
	@mkdir -p $(OUT_DIR)
	$(REDIS_LD) -o $@ $^ $(DEP_LIB_HIREDIS) $(DEP_OBJ_LINENOISE) $(DEP_LIB_HDR) $(FINAL_LIBS) $(TLS_CLIENT_LIBS)

.PHONY: redis-cli
redis-cli: $(REDIS_CLI_NAME)

# redis-benchmark
$(REDIS_BENCHMARK_NAME): $(REDIS_BENCHMARK_OBJ_OUT) $(REDIS_BENCHMARK_VEMB_LIB)
	@mkdir -p $(OUT_DIR)
	$(REDIS_LD) -o $@ $(REDIS_BENCHMARK_OBJ_OUT) $(REDIS_BENCHMARK_VEMB_LIB) $(DEP_LIB_HIREDIS) $(DEP_LIB_HDR) $(FINAL_LIBS) $(TLS_CLIENT_LIBS)

DEP = $(addprefix $(OUT_DIR)/,$(REDIS_SERVER_OBJ:%.o=%.d)) $(addprefix $(OUT_DIR)/,$(REDIS_VEC_SETS_OBJ:%.o=%.d)) $(addprefix $(OUT_DIR)/,$(REDIS_CLI_OBJ:%.o=%.d)) $(addprefix $(OUT_DIR)/,$(REDIS_BENCHMARK_OBJ:%.o=%.d))

$(OUT_DIR)/vemb_v16_client_sdk.o: ../clients/c/vemb_v16_client_sdk.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) $(FINAL_CFLAGS) $(FINAL_LDFLAGS) -MMD -o $@ -c $<
$(OUT_DIR)/vemb_v16_cli_l0.o: ../clients/c/vemb_v16_cli_l0.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) $(FINAL_CFLAGS) $(FINAL_LDFLAGS) -MMD -o $@ -c $<
$(OUT_DIR)/vemb_v16_cli_l1.o: ../clients/c/vemb_v16_cli_l1.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) $(FINAL_CFLAGS) $(FINAL_LDFLAGS) -MMD -o $@ -c $<
$(OUT_DIR)/vemb_v16_cluster_core.o: ../clients/c/vemb_v16_cluster_core.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) $(FINAL_CFLAGS) $(FINAL_LDFLAGS) -MMD -o $@ -c $<
$(OUT_DIR)/vemb_v16_owner_session.o: ../clients/c/vemb_v16_owner_session.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) $(FINAL_CFLAGS) $(FINAL_LDFLAGS) -MMD -o $@ -c $<
$(OUT_DIR)/vemb_v16_ub_peer_view.o: ../clients/c/vemb_v16_ub_peer_view.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) $(FINAL_CFLAGS) $(FINAL_LDFLAGS) -MMD -o $@ -c $<
$(OUT_DIR)/vemb_v16_client_topology.o: vemb_v16_client_topology.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) $(FINAL_CFLAGS) $(FINAL_LDFLAGS) -MMD -o $@ -c $<
-include $(DEP)

# redis-benchmark.o needs the SDK headers (clients/c/build/include/) so it can
# call vemb_v16_serialize_* / vemb_v16_parse_* directly. Other objects in the
# benchmark build do not.
$(OUT_DIR)/redis-benchmark.o: redis-benchmark.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) $(REDIS_BENCHMARK_VEMB_INC) -MMD -MF $(OUT_DIR)/redis-benchmark.d -o $@ -c $<

# Because the jemalloc.h header is generated as a part of the jemalloc build,
# building it should complete before building any other object. Instead of
# depending on a single artifact, build all dependencies first.
$(OUT_DIR)/%.o: %.c .make-prerequisites
	@mkdir -p $(OUT_DIR)
	$(REDIS_CC) -MMD -MF $(OUT_DIR)/$*.d -o $@ -c $<

# The following files are checked in and don't normally need to be rebuilt. They
# are built only if python is available and their prereqs are modified.
ifneq (,$(PYTHON))
$(COMMANDS_DEF_FILENAME).def: commands/*.json ../utils/generate-command-code.py
	$(QUIET_GEN)$(PYTHON) ../utils/generate-command-code.py $(GEN_COMMANDS_FLAGS)

fmtargs.h: ../utils/generate-fmtargs.py
	$(QUITE_GEN)sed '/Everything below this line/,$$d' $@ > $@.tmp
	$(QUITE_GEN)$(PYTHON) ../utils/generate-fmtargs.py >> $@.tmp
	$(QUITE_GEN)mv $@.tmp $@
endif

commands.c: $(COMMANDS_DEF_FILENAME).def

clean:
	rm -rf $(OUT_DIR)
	rm -f Makefile.dep
	rm -f $(TLC_SERVERS_OUT)
	-(cd ../tests/modules && $(MAKE) clean)

.PHONY: clean

distclean: clean
	-(cd ../deps && $(MAKE) distclean)
	-(cd modules && $(MAKE) clean)
	-(cd ../tests/modules && $(MAKE) clean)
	-(rm -f .make-*)

.PHONY: distclean

# ============================================================
# TLC Server build targets
# ============================================================

TLC_SERVERS = \
    tlc_server tlc_unified_server tlc_aeron_server tlc_fc_server \
    tlc_dpdk_server tlc_v10_server tlc_v13_server tlc_v14_server \
    tlc_v16_server
TLC_SERVERS_OUT = $(addprefix $(OUT_DIR)/,$(TLC_SERVERS)) $(OUT_DIR)/tlc_urma_server
TLC_SERVERS_PATTERN = $(addprefix $(OUT_DIR)/,$(TLC_SERVERS))

# TLC Server compilation flags (always includes SVE2 + LSE)
TLC_CFLAGS  = -O3 -std=c11 -pthread -I. -march=armv8.2-a+sve+lse -DUSE_SVE2 -DUSE_TLC $(filter -DUSE_NPU -I%,$(REDIS_CFLAGS))
TLC_LDFLAGS = -pthread -lm -lpthread $(filter -L% -l%,$(FINAL_LIBS))

# Shared source dependency for all TLC servers
TLC_COMMON_DEPS = three_layer_cache_ub.c

# Static pattern rule for all TLC servers
$(TLC_SERVERS_PATTERN): $(OUT_DIR)/%: %.c $(TLC_COMMON_DEPS)
	@printf '    %b %b\n' $(LINKCOLOR)BUILD$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2
	@mkdir -p $(OUT_DIR)
	$(CC) $(TLC_CFLAGS) -o $@ $< $(TLC_COMMON_DEPS) $(TLC_LDFLAGS)

# Build liburma.so from urma.c
$(OUT_DIR)/liburma.so: urma.c urma.h
	@printf '    %b %b\n' $(LINKCOLOR)BUILD$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2
	@mkdir -p $(OUT_DIR)
	$(CC) $(TLC_CFLAGS) -fPIC -shared -o $@ urma.c

# tlc_urma_server links against liburma.so
$(OUT_DIR)/tlc_urma_server: tlc_urma_server.c $(OUT_DIR)/liburma.so $(TLC_COMMON_DEPS)
	@printf '    %b %b\n' $(LINKCOLOR)BUILD$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2
	$(CC) $(TLC_CFLAGS) -o $@ $< $(TLC_COMMON_DEPS) -L$(OUT_DIR) -lurma -Wl,-rpath,'$$ORIGIN' $(TLC_LDFLAGS)

VEMB_V16_MAX_CHANNELS ?= $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || echo 64)
VEMB_V16_EXTRA_CFLAGS ?=
VEMB_V16_CFLAGS = -O3 -Wall -Wextra -pthread -std=gnu11 -I. -DVEMB_V16_MAX_CHANNELS=$(VEMB_V16_MAX_CHANNELS)
ifeq ($(USE_SVE),yes)
VEMB_V16_CFLAGS += -DUSE_SVE -DUSE_ARM_SVE -march=armv8.2-a+sve
endif
ifneq ($(strip $(PROXY_REQUEST_BATCH)),)
VEMB_V16_CFLAGS += -DPROXY_REQUEST_BATCH=$(PROXY_REQUEST_BATCH)
endif
ifneq ($(strip $(PROXY_RESPONSE_BATCH)),)
VEMB_V16_CFLAGS += -DPROXY_RESPONSE_BATCH=$(PROXY_RESPONSE_BATCH)
endif
ifneq ($(strip $(PROXY_QUEUE_BATCH)),)
VEMB_V16_CFLAGS += -DPROXY_QUEUE_BATCH=$(PROXY_QUEUE_BATCH)
endif
VEMB_V16_CFLAGS += -DVEMB_V16_PROXY_AFFINITY_MODE=$(VEMB_V16_PROXY_AFFINITY_MODE)
VEMB_V16_CFLAGS += $(VEMB_V16_EXTRA_CFLAGS)

vemb_v16_server: vemb_v16_server.c futex.c futex.h vemb_v16_proxy.c vemb_v16_tcp_transport.c vemb_v16_aeron_transport.c vemb_v16_aeron_attach.c vemb_v16_storage.c vemb_v16_warm_region_layout.c vemb_v16_mapped_region.c vemb_v16_remote_meta.c vemb_v16_ub_rpc.c vemb_v16_topology.c vemb_v16_migration_outbox.c vemb_v16_net.c vemb_v16_supernode.c vemb_v16_stats.c vemb_v16_tlc.c tlc_core.c vemb_v16_warm_provider.c vemb_v16_log.c sve_operation.c sve_similarity.c localtime.c zmalloc.c monotonic.c redisassert.c strl.c vemb_v16_proxy.h vemb_v16_proxy_internal.h vemb_v16_proxy_types.h vemb_v16_tcp_transport.h vemb_v16_aeron_transport.h vemb_v16_aeron_attach.h vemb_v16_storage.h vemb_v16_warm_region_layout.h vemb_v16_mapped_region.h vemb_v16_remote_meta.h vemb_v16_ub_rpc.h vemb_v16_topology.h vemb_v16_migration_outbox.h vemb_v16_protocol.h vemb_v16_net.h vemb_v16_dataplane.h vemb_v16_aeron_ring.h vemb_v16_client_ring.h vemb_v16_supernode.h vemb_v16_stats.h vemb_v16_tlc.h tlc_core.h vemb_v16_warm_provider.h vemb_v16_log.h sve_operation.h sve_similarity.h
	@echo 'WARNING: vemb_v16_server is deprecated; do not use it for performance benchmarks. Use redis-server + memtier_benchmark.' 1>&2
	@printf '    %b %b\n' $(LINKCOLOR)BUILD$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR) 1>&2
	$(CC) $(VEMB_V16_CFLAGS) -o $@ vemb_v16_server.c futex.c vemb_v16_proxy.c vemb_v16_tcp_transport.c vemb_v16_aeron_transport.c vemb_v16_aeron_attach.c vemb_v16_storage.c vemb_v16_warm_region_layout.c vemb_v16_mapped_region.c vemb_v16_remote_meta.c vemb_v16_ub_rpc.c vemb_v16_topology.c vemb_v16_migration_outbox.c vemb_v16_net.c vemb_v16_supernode.c vemb_v16_stats.c vemb_v16_tlc.c tlc_core.c vemb_v16_warm_provider.c vemb_v16_log.c sve_operation.c sve_similarity.c localtime.c zmalloc.c monotonic.c redisassert.c strl.c -pthread -lm

ifdef TLC_VERSION
tlc_single: $(OUT_DIR)/tlc_$(TLC_VERSION)_server
endif

# Build subset: make TLC_VERSIONS="v13 v14"
ifdef TLC_VERSIONS
tlc_subset:
	$(foreach v,$(TLC_VERSIONS),$(MAKE) $(OUT_DIR)/tlc_$(v)_server &&) true
endif

# Build all TLC servers
tlc-all: $(TLC_SERVERS_OUT)

tlc-clean:
	rm -f $(TLC_SERVERS_OUT) $(OUT_DIR)/liburma.so

.PHONY: tlc-all tlc-clean tlc_single tlc_subset

# ============================================================
# Help target
# ============================================================

help:
	@echo "Redis HPC Build System"
	@echo ""
	@echo "Usage:"
	@echo "  make                                    Build redis-server (default, no HPC)"
	@echo "  make USE_SVE2=yes USE_LSE=yes           Build with SVE2 + LSE atomics"
	@echo "  make USE_SVE2=yes USE_NPU=yes           Build with SVE2 + Ascend NPU"
	@echo "  make USE_SVE2=yes USE_UB=yes USE_TLC=yes  Full HPC build"
	@echo "  make ub_client_ut                       Build ub_client UT (in benchmark/)"
	@echo ""
	@echo "  make TLC_VERSION=v14                    Build single TLC server"
	@echo "  make TLC_VERSIONS=\"v13 v14\"             Build specific TLC servers"
	@echo "  make tlc-all                            Build all 9 TLC servers"
	@echo ""
	@echo "Feature flags:"
	@echo "  USE_SVE2=yes    ARM SVE2 vector instructions"
	@echo "  USE_LSE=yes     ARM LSE atomic instructions"
	@echo "  USE_F32MM=yes   SVE F32MM matrix extension (requires SVE2)"
	@echo "  USE_NPU=yes     Huawei Ascend NPU (requires ASCEND_HOME)"
	@echo "  USE_UB=yes      UB bus components"
	@echo "  USE_TLC=yes     Three-layer cache module"
	@echo ""
	@echo "Paths:"
	@echo "  ASCEND_HOME     Ascend Toolkit root (default: /usr/local/Ascend/ascend-toolkit/latest)"

.PHONY: help

test: $(REDIS_SERVER_NAME) $(REDIS_CHECK_AOF_NAME) $(REDIS_CLI_NAME) $(REDIS_BENCHMARK_NAME) module_tests
	@(cd ..; ./runtest)

test-modules: $(REDIS_SERVER_NAME)
	@(cd ..; ./runtest-moduleapi)

test-sentinel: $(REDIS_SENTINEL_NAME) $(REDIS_CLI_NAME)
	@(cd ..; ./runtest-sentinel)

test-cluster: $(REDIS_SERVER_NAME) $(REDIS_CLI_NAME)
	@(cd ..; ./runtest-cluster)

check: test

lcov:
	@lcov --version
	$(MAKE) gcov
	@(set -e; cd ..; ./runtest)
	@geninfo -o redis.info .
	@genhtml --legend -o lcov-html redis.info

.PHONY: lcov

bench: $(REDIS_BENCHMARK_NAME)
	./$(REDIS_BENCHMARK_NAME)

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) CFLAGS="-m32" LDFLAGS="-m32" SKIP_VEC_SETS="yes"

gcov:
	$(MAKE) REDIS_CFLAGS="-fprofile-arcs -ftest-coverage -DCOVERAGE_TEST" REDIS_LDFLAGS="-fprofile-arcs -ftest-coverage"

noopt:
	$(MAKE) OPTIMIZATION="-O0"

valgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc"

helgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc" CFLAGS="-D__ATOMIC_VAR_FORCE_SYNC_MACROS" REDIS_CFLAGS="-I/usr/local/include" REDIS_LDFLAGS="-L/usr/local/lib"

install: all
	@mkdir -p $(INSTALL_BIN)
	$(call MAKE_INSTALL,$(REDIS_SERVER_NAME),$(INSTALL_BIN))
	$(call MAKE_INSTALL,$(REDIS_BENCHMARK_NAME),$(INSTALL_BIN))
	$(call MAKE_INSTALL,$(REDIS_CLI_NAME),$(INSTALL_BIN))
	@ln -sf $(REDIS_SERVER_NAME) $(INSTALL_BIN)/$(REDIS_CHECK_RDB_NAME)
	@ln -sf $(REDIS_SERVER_NAME) $(INSTALL_BIN)/$(REDIS_CHECK_AOF_NAME)
	@ln -sf $(REDIS_SERVER_NAME) $(INSTALL_BIN)/$(REDIS_SENTINEL_NAME)

uninstall:
	rm -f $(INSTALL_BIN)/{$(REDIS_SERVER_NAME),$(REDIS_BENCHMARK_NAME),$(REDIS_CLI_NAME),$(REDIS_CHECK_RDB_NAME),$(REDIS_CHECK_AOF_NAME),$(REDIS_SENTINEL_NAME)}

# ============================================================
# Hash Bench Server — pluggable hash strategy testing
# ============================================================
USE_UB_MEM ?= 1
HOT_ON_UB ?= 1
HBENCH_CFLAGS = -O3 -std=c11 -pthread -I. -DUSE_TLC $(if $(filter aarch64 armv%,$(shell uname -m)),-march=armv8.2-a+sve+lse -DUSE_SVE2,)
ifeq ($(USE_UB_MEM),1)
HBENCH_CFLAGS += -DUSE_UB_MEM -DUB_CLIENT_STANDALONE -DHOT_ON_UB=$(HOT_ON_UB)
ifdef UB_MEMID_BASE
HBENCH_CFLAGS += -DUB_MEMID_BASE=$(UB_MEMID_BASE)
endif
ifdef UB_CACHEABLE
HBENCH_CFLAGS += -DUB_CACHEABLE=$(UB_CACHEABLE)
endif
ifdef UB_SHM_SIZE
HBENCH_CFLAGS += -DUB_SHM_SIZE=$(UB_SHM_SIZE)
endif
ifdef UB_HOT_MEMID
HBENCH_CFLAGS += -DUB_HOT_MEMID=$(UB_HOT_MEMID)
endif
ifdef UB_HOT_CACHEABLE
HBENCH_CFLAGS += -DUB_HOT_CACHEABLE=$(UB_HOT_CACHEABLE)
endif
ifdef UB_HOT_SLICE_SIZE
HBENCH_CFLAGS += -DUB_HOT_SLICE_SIZE=$(UB_HOT_SLICE_SIZE)
endif
endif
HBENCH_LDFLAGS = -pthread -lm -lpthread

# FC (Flat Combining) version — v16-based architecture
# Hash strategy: v1=1, v2=2, v2fixed=3, v2full64=4
# Eviction strategy: blind=0, clock=1
FC_BENCH_SRCS = tlc_hash_fc_server.c tlc_hash_fc_cache.c
ifeq ($(USE_UB_MEM),1)
FC_BENCH_SRCS += ub_client.c
endif

define FC_BENCH_RULE
$(OUT_DIR)/tlc_hash_fc_server_$(1)_$(2): $$(FC_BENCH_SRCS) hash_strategy.h eviction_strategy.h
	@mkdir -p $(OUT_DIR)
	$$(CC) $$(HBENCH_CFLAGS) -DHASH_STRATEGY=$(3) -DEVICTION_STRATEGY=$(4) -o $$@ $$(FC_BENCH_SRCS) $$(HBENCH_LDFLAGS)

$(OUT_DIR)/tlc_hash_fc_server_$(1)_$(2)_stats: $$(FC_BENCH_SRCS) hash_strategy.h eviction_strategy.h
	@mkdir -p $(OUT_DIR)
	$$(CC) $$(HBENCH_CFLAGS) -DHASH_STRATEGY=$(3) -DEVICTION_STRATEGY=$(4) -DENABLE_COLLISION_STATS -o $$@ $$(FC_BENCH_SRCS) $$(HBENCH_LDFLAGS)
endef

$(eval $(call FC_BENCH_RULE,v1,blind,1,0))
$(eval $(call FC_BENCH_RULE,v1,clock,1,1))
$(eval $(call FC_BENCH_RULE,v1,rr,1,2))
$(eval $(call FC_BENCH_RULE,v2,blind,2,0))
$(eval $(call FC_BENCH_RULE,v2,clock,2,1))
$(eval $(call FC_BENCH_RULE,v2,rr,2,2))
$(eval $(call FC_BENCH_RULE,v2fixed,blind,3,0))
$(eval $(call FC_BENCH_RULE,v2fixed,clock,3,1))
$(eval $(call FC_BENCH_RULE,v2fixed,rr,3,2))
$(eval $(call FC_BENCH_RULE,v2full64,blind,4,0))
$(eval $(call FC_BENCH_RULE,v2full64,clock,4,1))
$(eval $(call FC_BENCH_RULE,v2full64,rr,4,2))
$(eval $(call FC_BENCH_RULE,v3oddeven,blind,5,0))
$(eval $(call FC_BENCH_RULE,v3oddeven,clock,5,1))
$(eval $(call FC_BENCH_RULE,v3oddeven,rr,5,2))

FC_BENCH_ALL = $(foreach h,v1 v2 v2fixed v2full64 v3oddeven,$(addprefix $(OUT_DIR)/tlc_hash_fc_server_$(h)_,blind blind_stats clock clock_stats rr rr_stats))

hash-bench-server: $(FC_BENCH_ALL)
	@echo "Built 30 targets: 5 hash × 3 eviction × 2 (perf/stats)"

hash-bench-clean:
	rm -f $(OUT_DIR)/tlc_hash_fc_server_*

.PHONY: hash-bench-server hash-bench-clean
