# contrib/workmem_dtree/Makefile

MODULE_big = workmem_dtree
OBJS = workmem_dtree.o workmem_model.o

EXTENSION = workmem_dtree
DATA = workmem_dtree--1.0.sql

REGRESS = workmem_dtree

exclude_option = -fPIE
override CPPFLAGS := $(filter-out $(exclude_option),$(CPPFLAGS)) -fPIC
override CFLAGS := $(filter-out $(exclude_option),$(CFLAGS)) -fPIC

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/workmem_dtree
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
