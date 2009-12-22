AM_CFLAGS = -I$(top_srcdir)/src

include_HEADERS = scr.h

bin_PROGRAMS = scr_rebuild_xor scr_halt_cntl scr_crc32 scr_log_event scr_log_transfer

lib_LTLIBRARIES = libscr.la 
libscr_la_SOURCES = \
	scr.h \
	scr_err.h \
	scr_hash.c     scr_hash.h \
	scr_filemap.c  scr_filemap.h \
	scr_param.c    scr_param.h \
	scr_log.c      scr_log.h \
	scr_copy_xor.c scr_copy_xor.h \
	scr_io.c       scr_io.h \
	scr_meta.c     scr_meta.h \
	scr_halt.c     scr_halt.h \
	scr.c
libscr_la_LIBADD = $(YOGRT_LDFLAGS) $(YOGRT_LIBS) $(MYSQL_LDFLAGS) $(MYSQL_LIBS) -lz
libscr_la_CFLAGS = $(AM_CFLAGS) $(YOGRT_CPPFLAGS) $(MYSQL_CPPFLAGS)

lib_LTLIBRARIES += libscr_interpose.la
libscr_interpose_la_SOURCES = \
	scr.h \
	scr_err.h \
	scr_hash.c     scr_hash.h \
	scr_filemap.c  scr_filemap.h \
	scr_param.c    scr_param.h \
	scr_log.c      scr_log.h \
	scr_copy_xor.c scr_copy_xor.h \
	scr_io.c       scr_io.h \
	scr_meta.c     scr_meta.h \
	scr_halt.c     scr_halt.h \
	scr.c \
	scr_interpose.c
libscr_interpose_la_LIBADD = $(YOGRT_LDFLAGS) $(YOGRT_LIBS) $(MYSQL_LDFLAGS) $(MYSQL_LIBS) -lz
libscr_interpose_la_CFLAGS = $(AM_CFLAGS) $(YOGRT_CPPFLAGS)

scr_rebuild_xor_SOURCES = \
	scr.h \
	scr_err_serial.c scr_err.h \
	scr_io.c         scr_io.h \
	scr_meta.c       scr_meta.h \
	scr_filemap.c    scr_filemap.h \
	scr_hash.c       scr_hash.h \
	scr_copy_xor.c   scr_copy_xor.h \
	scr_rebuild_xor.c
scr_rebuild_xor_LDADD = -lz
scr_rebuild_xor_CFLAGS = $(AM_CFLAGS)

scr_halt_cntl_SOURCES = \
	scr.h \
	scr_err_serial.c scr_err.h \
	scr_io.c         scr_io.h \
	scr_halt.c       scr_halt.h \
	scr_halt_cntl.c
scr_halt_cntl_LDADD = -lz
scr_halt_cntl_CFLAGS = $(AM_CFLAGS)

scr_crc32_SOURCES = \
	scr.h \
	scr_err_serial.c scr_err.h \
	scr_io.c         scr_io.h \
	scr_crc32.c
scr_crc32_LDADD = -lz
scr_crc32_CFLAGS = $(AM_CFLAGS)

scr_log_event_SOURCES = \
	scr.h \
	scr_err_serial.c scr_err.h \
	scr_hash.c     scr_hash.h \
	scr_param.c    scr_param.h \
	scr_log.c        scr_log.h \
	scr_io.c         scr_io.h \
	scr_log_event.c
scr_log_event_LDADD = -lz $(MYSQL_LDFLAGS) $(MYSQL_LIBS)
scr_log_event_CFLAGS = $(AM_CFLAGS)

scr_log_transfer_SOURCES = \
	scr.h \
	scr_err_serial.c scr_err.h \
	scr_hash.c     scr_hash.h \
	scr_param.c    scr_param.h \
	scr_log.c        scr_log.h \
	scr_io.c         scr_io.h \
	scr_log_transfer.c
scr_log_transfer_LDADD = -lz $(MYSQL_LDFLAGS) $(MYSQL_LIBS)
scr_log_transfer_CFLAGS = $(AM_CFLAGS)

EXTRA_DIST = queue.h
