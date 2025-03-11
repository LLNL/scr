AM_CFLAGS = -I$(top_srcdir)/src -I$(top_srcdir)/config

# headers to be installed in /include subdirectory
include_HEADERS = \
	dtcmp.h

# headers that should not be installed into /include
noinst_HEADERS = \
	dtcmp_internal.h \
	dtcmp_ops.h

lib_LTLIBRARIES = \
	libdtcmp.la

INCLUDES = \
	$(DEFS) \
	$(DEFAULT_INCLUDES) \
	$(AM_CPPFLAGS) $(CPPFLAGS) \
	$(AM_CFLAGS) $(CFLAGS) \
	$(MPI_CFLAGS) $(LWGRP_CFLAGS)

# source files that are used by scr commands and runtime library
libdtcmp_la_SOURCES = \
  dtcmp.c \
  dtcmp_util.c \
  dtcmp_ops.c \
  dtcmp_uniqify.c \
  dtcmp_search_binary.c \
  dtcmp_partitionz.c \
  dtcmp_partitionz_list.c \
  dtcmp_partition_local.c \
  dtcmp_merge_2way.c \
  dtcmp_merge_kway_heap.c \
  dtcmp_select_local_ends.c \
  dtcmp_select_local_randpartition.c \
  dtcmp_selectv_rand.c \
  dtcmp_selectv_medianofmedians.c \
  dtcmp_is_sorted.c \
  dtcmp_sort_local_insertionsort.c \
  dtcmp_sort_local_randquicksort.c \
  dtcmp_sort_local_mergesort.c \
  dtcmp_sort_local_qsort.c \
  dtcmp_sort_allgather.c \
  dtcmp_sort_bitonic.c \
  dtcmp_sort_samplesort.c \
  dtcmp_sortv_allgather.c \
  dtcmp_sortv_sortgather_scatter.c \
  dtcmp_sortv_cheng.c \
  dtcmp_sortz_samplesort.c \
  dtcmp_rankv_sort.c \
  dtcmp_seg_exscan.c
libdtcmp_la_CFLAGS = $(INCLUDES)
libdtcmp_la_LIBADD = $(LWGRP_LDFLAGS) $(LWGRP_LIBS)
libdtcmp_la_LDFLAGS = -avoid-version
