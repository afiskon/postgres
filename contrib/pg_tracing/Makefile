# contrib/pg_tracing/Makefile

MODULE_big = pg_tracing
OBJS = \
	$(WIN32RES) \
	pg_tracing.o

EXTENSION = pg_tracing
TAP_TESTS = 0
PGFILEDESC = "pg_tracing - Distributed tracing for postgres"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_tracing
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
