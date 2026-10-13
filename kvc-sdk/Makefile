# kvc-sdk Makefile
#
# 产物:
#   build/libkvc_server.{so,a}  — core + server（mooncake_client / master 链接）
#   build/libkvc_client.{so,a}  — core + server + client（推理框架连接）
#   build/kvc_smoke, build/kvc_probe — 测试/探针工具
#
# 编译口径与 clients/c/Makefile 对齐: aarch64 加 SVE。

CC      ?= cc
AR      ?= ar
CFLAGS  ?= -O2 -g -fPIC -Wall -Wextra
LDLIBS  = -lpthread -lm
UNAME_M := $(shell uname -m)

ifeq ($(UNAME_M),aarch64)
CFLAGS += -DUSE_ARM_SVE -march=armv8.2-a+sve
endif

INC := -Iinclude -Icore/layout -Icore/ub -Iserver

CORE_SRC  = core/layout/kvc_layout.c core/ub/kvc_ub.c
SERVER_SRC = $(CORE_SRC) server/kvc_server.c
CLIENT_SRC = $(SERVER_SRC) client/kvc_client.c

# ubme seam（provider=UBME 的动态段支撑; dlopen 合作方 libubme.so，
# 缺运行时的环境上 UBME provider 初始化显性失败，DEVICE/ANON 不受影响）
SEAM_OBJ = build/ubme_seam.o
SERVER_OBJ = $(SERVER_SRC:.c=.o) $(SEAM_OBJ)
CLIENT_OBJ = $(CLIENT_SRC:.c=.o) $(SEAM_OBJ)
LDLIBS += -ldl

HDRS = $(wildcard include/kvc/*.h) core/layout/kvc_layout.h core/ub/kvc_ub.h \
       core/ub/ubme_seam.h server/kvc_region_internal.h

.PHONY: all check clean tools

all: check libs tools

# --- 头文件自包含性检查 ----------------------------------------------------
check: build/headers_check
	./build/headers_check && echo "headers check: OK"

build/headers_check: tests/headers_check.c $(wildcard include/kvc/*.h)
	@mkdir -p build
	$(CC) $(CFLAGS) -Iinclude -o $@ tests/headers_check.c

# --- 库 --------------------------------------------------------------------
libs: build/libkvc_server.a build/libkvc_server.so \
      build/libkvc_client.a build/libkvc_client.so

build/libkvc_server.a: $(SERVER_OBJ)
	$(AR) rcs $@ $^
build/libkvc_server.so: $(SERVER_OBJ)
	$(CC) -shared -o $@ $^ $(LDLIBS)
build/libkvc_client.a: $(CLIENT_OBJ)
	$(AR) rcs $@ $^
build/libkvc_client.so: $(CLIENT_OBJ)
	$(CC) -shared -o $@ $^ $(LDLIBS)

build/ubme_seam.o: core/ub/ubme_seam.c core/ub/ubme_seam.h $(HDRS)
	@mkdir -p build
	$(CC) $(CFLAGS) $(INC) -c -o $@ $<

%.o: %.c $(HDRS)
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) $(INC) -c -o $@ $<

# --- 工具 ------------------------------------------------------------------
tools: build/kvc_smoke build/kvc_probe build/kvc_nc_bw

build/kvc_smoke: tests/kvc_smoke.c build/libkvc_client.a
	$(CC) $(CFLAGS) -Iinclude -o $@ tests/kvc_smoke.c build/libkvc_client.a $(LDLIBS)

build/kvc_probe: tests/kvc_probe.c build/libkvc_server.a
	$(CC) $(CFLAGS) -Iinclude -o $@ tests/kvc_probe.c build/libkvc_server.a $(LDLIBS)

build/kvc_nc_bw: tests/kvc_nc_bw.c build/libkvc_server.a
	$(CC) $(CFLAGS) -Iinclude -o $@ tests/kvc_nc_bw.c build/libkvc_server.a $(LDLIBS)


clean:
	rm -rf build *.o core/layout/*.o core/ub/*.o server/*.o client/*.o
