MPICC     = @MPICC@

AM_CFLAGS = -I$(top_srcdir)/src

include_HEADERS = ../scr.h

bin_PROGRAMS = \
	scr_index

scr_index_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_meta.c        ../scr_meta.h \
	../scr_filemap.c     ../scr_filemap.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	scr_index.c
scr_index_LDADD = -lz
scr_index_CFLAGS = $(AM_CFLAGS)
