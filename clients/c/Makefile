# clients/c/Makefile — build libvemb_v16_client.{a,so} + install public headers
#
# Usage:
#   make              # build .a, .so, install headers
#   make static       # only .a
#   make shared       # only .so
#   make install-headers  # only flatten headers to build/include/
#   make example      # link example.c against .a (validation)
#   make example_so   # link example.c against .so (validation, uses rpath)
#   make clean
#
# External projects consume via:
#   -I<clients/c>/build/include -L<clients/c>/build -lvemb_v16_client

# ---- toolchain ----
CC ?= cc
AR ?= ar
CFLAGS ?= -O2 -g
WARN_FLAGS = -Wall -Wno-unused-function -Wno-unused-variable
PIC_FLAGS = -fPIC
# _GNU_SOURCE is defined at the top of each .c file; we do NOT add -D_GNU_SOURCE
# here to avoid a redefinition warning.

# AArch64: enable SVE so sve_streaming_load_f32 uses vector loads instead of
# plain memcpy (matches redis-server FINAL_CFLAGS in src/Makefile:182).
ARCH_MACHINE := $(shell uname -m)
ifeq ($(ARCH_MACHINE),aarch64)
ARCH_FLAGS ?= -DUSE_ARM_SVE -march=armv8.2-a+sve
else
ARCH_FLAGS ?=
endif
ARCH_FLAGS += -DVEMB_V16_CLIENT_RING_USE_SVE

# ---- paths (absolute, so `make -C` works from anywhere) ----
THIS_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
SDK_DIR := $(THIS_DIR)
SRC_DIR := $(abspath $(THIS_DIR)/../../src)
INTERNAL_DIR := $(THIS_DIR)/internal

# Include search order: SDK dir first, then internal/ (overrides src/macro.h),
# then src/ (for protocol.h, net.h, etc.).
INCLUDES := -I$(SDK_DIR) -I$(INTERNAL_DIR) -I$(SRC_DIR)

# ---- sources (4 files comprising the SDK) ----
SDK_SOURCES := \
	$(SDK_DIR)/vemb_v16_client_sdk.c \
	$(SRC_DIR)/vemb_v16_net.c \
	$(SRC_DIR)/vemb_v16_client_topology.c \
	$(SRC_DIR)/vemb_v16_topology.c

# ---- build dirs ----
BUILD_DIR := build
OBJ_DIR := $(BUILD_DIR)/obj
LIB_DIR := $(BUILD_DIR)
INC_DIR := $(BUILD_DIR)/include

# Objects: flatten via basename (all 4 names are distinct)
SDK_OBJS := $(patsubst %.c,$(OBJ_DIR)/%.o,$(notdir $(SDK_SOURCES)))
DEP_FILES := $(SDK_OBJS:.o=.d)

# ---- public headers (flat install) ----
PUBLIC_HEADERS := \
	$(INC_DIR)/vemb_v16_client_sdk.h \
	$(INC_DIR)/macro.h \
	$(INC_DIR)/vemb_v16_protocol.h \
	$(INC_DIR)/vemb_v16_cacheline.h \
	$(INC_DIR)/vemb_v16_net.h \
	$(INC_DIR)/vemb_v16_client_topology.h \
	$(INC_DIR)/vemb_v16_topology.h \
	$(INC_DIR)/vemb_v16_hash.h \
	$(INC_DIR)/vemb_v16_peer_view_map.h

# ---- compile flags shared across all .c ----
ALL_CFLAGS := $(CFLAGS) $(ARCH_FLAGS) $(PIC_FLAGS) $(WARN_FLAGS) $(INCLUDES)

# ---- targets ----
.PHONY: all static shared install-headers example example_so correctness clean

all: static shared install-headers

# Compile rule: SDK-local .c (vemb_v16_client_sdk.c)
$(OBJ_DIR)/vemb_v16_client_sdk.o: $(SDK_DIR)/vemb_v16_client_sdk.c
	@mkdir -p $(OBJ_DIR)
	$(CC) $(ALL_CFLAGS) -MMD -c $< -o $@

# Compile rule: src/ .c files (vemb_v16_net.c, vemb_v16_client_topology.c, vemb_v16_topology.c)
$(OBJ_DIR)/vemb_v16_net.o: $(SRC_DIR)/vemb_v16_net.c
	@mkdir -p $(OBJ_DIR)
	$(CC) $(ALL_CFLAGS) -MMD -c $< -o $@

$(OBJ_DIR)/vemb_v16_client_topology.o: $(SRC_DIR)/vemb_v16_client_topology.c
	@mkdir -p $(OBJ_DIR)
	$(CC) $(ALL_CFLAGS) -MMD -c $< -o $@

$(OBJ_DIR)/vemb_v16_topology.o: $(SRC_DIR)/vemb_v16_topology.c
	@mkdir -p $(OBJ_DIR)
	$(CC) $(ALL_CFLAGS) -MMD -c $< -o $@

# Static library
static: $(LIB_DIR)/libvemb_v16_client.a

$(LIB_DIR)/libvemb_v16_client.a: $(SDK_OBJS)
	@mkdir -p $(LIB_DIR)
	$(AR) rcs $@ $^

# Shared library
shared: $(LIB_DIR)/libvemb_v16_client.so

$(LIB_DIR)/libvemb_v16_client.so: $(SDK_OBJS)
	@mkdir -p $(LIB_DIR)
	$(CC) -shared -o $@ $^

# Flat header install
install-headers: $(PUBLIC_HEADERS)

$(INC_DIR)/vemb_v16_client_sdk.h: $(SDK_DIR)/vemb_v16_client_sdk.h
	@mkdir -p $(INC_DIR)
	# Rewrite ../../src/ prefixes to path-free includes so external
	# consumers only need -I<inc-dir>, not -I<repo>/src.
	sed -e 's|#include "../../src/|#include "|g' $< > $@

$(INC_DIR)/macro.h: $(INTERNAL_DIR)/macro.h
	@mkdir -p $(INC_DIR)
	cp $< $@

$(INC_DIR)/vemb_v16_protocol.h: $(SRC_DIR)/vemb_v16_protocol.h
	@mkdir -p $(INC_DIR)
	cp $< $@

$(INC_DIR)/vemb_v16_cacheline.h: $(SRC_DIR)/vemb_v16_cacheline.h
	@mkdir -p $(INC_DIR)
	cp $< $@

$(INC_DIR)/vemb_v16_net.h: $(SRC_DIR)/vemb_v16_net.h
	@mkdir -p $(INC_DIR)
	cp $< $@

$(INC_DIR)/vemb_v16_client_topology.h: $(SRC_DIR)/vemb_v16_client_topology.h
	@mkdir -p $(INC_DIR)
	cp $< $@

$(INC_DIR)/vemb_v16_topology.h: $(SRC_DIR)/vemb_v16_topology.h
	@mkdir -p $(INC_DIR)
	cp $< $@

$(INC_DIR)/vemb_v16_hash.h: $(SRC_DIR)/vemb_v16_hash.h
	@mkdir -p $(INC_DIR)
	cp $< $@

$(INC_DIR)/vemb_v16_peer_view_map.h: $(SRC_DIR)/vemb_v16_peer_view_map.h
	@mkdir -p $(INC_DIR)
	cp $< $@

# ---- validation: link example.c against the library ----
# NOTE: for the static variant we pass the .a path directly (not -l), because
# the linker otherwise prefers libvemb_v16_client.so when both are present.
example: $(LIB_DIR)/libvemb_v16_client.a install-headers
	$(CC) $(CFLAGS) -I$(INC_DIR) -o $(BUILD_DIR)/example example.c \
		$(LIB_DIR)/libvemb_v16_client.a

example_so: $(LIB_DIR)/libvemb_v16_client.so install-headers
	$(CC) $(CFLAGS) -I$(INC_DIR) -o $(BUILD_DIR)/example_so example.c \
		-L$(LIB_DIR) -lvemb_v16_client \
		-Wl,-rpath,$(LIB_DIR)

# Multi-region Aeron correctness test — compiled from source tree (not flat
# install) so vemb_v16_hash.h → xxhash.h relative includes resolve.
correctness: $(LIB_DIR)/libvemb_v16_client.a
	$(CC) $(CFLAGS) $(ARCH_FLAGS) $(INCLUDES) \
		-o $(BUILD_DIR)/sdk_multi_region_correctness sdk_multi_region_correctness.c \
		$(LIB_DIR)/libvemb_v16_client.a -lpthread -lm

clean:
	rm -rf $(BUILD_DIR)

# ---- auto dependency tracking ----
-include $(DEP_FILES)
