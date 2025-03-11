AM_CFLAGS = -I$(top_srcdir)/src -I$(top_srcdir)/config

# headers to be installed in /include subdirectory
include_HEADERS = \
	lwgrp.h

# headers that should not be installed into /include
noinst_HEADERS = \
	lwgrp_internal.h

lib_LTLIBRARIES = \
	liblwgrp.la

INCLUDES = \
	$(DEFS) \
	$(DEFAULT_INCLUDES) \
	$(AM_CPPFLAGS) $(CPPFLAGS) \
	$(AM_CFLAGS) $(CFLAGS) \
	$(MPI_CFLAGS)

# source files that are used by scr commands and runtime library
liblwgrp_la_SOURCES = \
  lwgrp.c \
  lwgrp_util.c \
  lwgrp_chain_ops.c \
  lwgrp_ring_ops.c \
  lwgrp_logchain_ops.c \
  lwgrp_logring_ops.c \
  lwgrp_comm.c \
  lwgrp_comm_split.c
liblwgrp_la_CFLAGS = $(INCLUDES)
liblwgrp_la_LIBADD =
liblwgrp_la_LDFLAGS = -avoid-version
