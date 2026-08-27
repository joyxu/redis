# src/build_opts/sve.mk — ARM SVE gather acceleration for ub_client
# SVE is the default on the ARM hosts used by VEMB. Pass USE_SVE=no to opt out.

USE_SVE ?= yes

ifeq ($(USE_SVE),yes)
    FEATURE_MARCH_SUFFIX += +sve
    FEATURE_CFLAGS  += -DUSE_SVE
endif
