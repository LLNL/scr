# TODO: move -lz to m4 script

AM_CFLAGS = -I$(top_srcdir)/src

include_HEADERS = ../scr.h

noinst_HEADERS = \
	../scr_conf.h \
	../scr_err.h \
	../scr_io.h \
	../scr_util.h \
	../scr_hash.h \
	../scr_hash_util.h \
	../tv_data_display.h \
	../scr_filemap.h \
	../scr_index_api.h \
	../scr_meta.h \
	../scr_config.h \
	../scr_param.h \
	../scr_log.h \
	../scr_copy_xor.h \
	../scr_halt.h

bin_PROGRAMS = \
	scr_inspect_cache \
	scr_copy \
	scr_rebuild_xor \
	scr_halt_cntl \
	scr_flush_file \
	scr_nodes_file \
	scr_retries_halt \
	scr_crc32 \
	scr_print_hash_file \
	scr_log_event \
	scr_log_transfer \
	scr_transfer \
	scr_index

scr_copy_SOURCES = scr_copy.c
scr_copy_LDADD = -lz libscrcmds.la
scr_copy_CFLAGS = $(AM_CFLAGS)

scr_inspect_cache_SOURCES = scr_inspect_cache.c
scr_inspect_cache_LDADD = -lz libscrcmds.la
scr_inspect_cache_CFLAGS = $(AM_CFLAGS)

scr_rebuild_xor_SOURCES = scr_rebuild_xor.c
scr_rebuild_xor_LDADD = -lz libscrcmds.la
scr_rebuild_xor_CFLAGS = $(AM_CFLAGS)

scr_halt_cntl_SOURCES = scr_halt_cntl.c
scr_halt_cntl_LDADD = -lz libscrcmds.la
scr_halt_cntl_CFLAGS = $(AM_CFLAGS)

scr_flush_file_SOURCES = scr_flush_file.c
scr_flush_file_SOURCES = scr_flush_file.c
scr_flush_file_LDADD = -lz libscrcmds.la
scr_flush_file_CFLAGS = $(AM_CFLAGS)

scr_nodes_file_SOURCES = scr_nodes_file.c
scr_nodes_file_LDADD = -lz libscrcmds.la
scr_nodes_file_CFLAGS = $(AM_CFLAGS)

scr_retries_halt_SOURCES = scr_retries_halt.c
scr_retries_halt_LDADD = -lz libscrcmds.la
scr_retries_halt_CFLAGS = $(AM_CFLAGS)

scr_crc32_SOURCES = scr_crc32.c
scr_crc32_LDADD = -lz libscrcmds.la
scr_crc32_CFLAGS = $(AM_CFLAGS)

scr_print_hash_file_SOURCES = scr_print_hash_file.c
scr_print_hash_file_LDADD = -lz libscrcmds.la
scr_print_hash_file_CFLAGS = $(AM_CFLAGS)

scr_log_event_SOURCES = scr_log_event.c
scr_log_event_LDADD = -lz libscrcmds.la $(MYSQL_LDFLAGS) $(MYSQL_LIBS)
scr_log_event_CFLAGS = $(AM_CFLAGS)

scr_log_transfer_SOURCES = scr_log_transfer.c
scr_log_transfer_LDADD = -lz libscrcmds.la $(MYSQL_LDFLAGS) $(MYSQL_LIBS)
scr_log_transfer_CFLAGS = $(AM_CFLAGS)

scr_transfer_SOURCES = scr_transfer.c
scr_transfer_LDADD = -lz libscrcmds.la
scr_transfer_CFLAGS = $(AM_CFLAGS)

scr_index_SOURCES = scr_index.c
scr_index_LDADD = -lz libscrcmds.la
scr_index_CFLAGS = $(AM_CFLAGS)

INCLUDES = \
	$(DEFS) \
	$(DEFAULT_INCLUDES) \
	$(AM_CPPFLAGS) $(CPPFLAGS) \
	$(AM_CFLAGS) $(CFLAGS) \
	$(MYSQL_CPPFLAGS)

lib_LTLIBRARIES = libscrcmds.la

scr_sources = \
	../scr_io.c \
	../scr_util.c \
	../scr_hash.c \
	../scr_hash_util.c \
	../tv_data_display.c \
	../scr_filemap.c \
	../scr_index_api.c \
	../scr_meta.c \
	../scr_config.c \
	../scr_param.c \
	../scr_log.c \
	../scr_copy_xor.c \
	../scr_halt.c \
	../scr_err_serial.c \
	../scr_config_serial.c

scr_objs = \
	../scr_io.lo \
	../scr_util.lo \
	../scr_hash.lo \
	../scr_hash_util.lo \
	../tv_data_display.lo \
	../scr_filemap.lo \
	../scr_index_api.lo \
	../scr_meta.lo \
	../scr_config.lo \
	../scr_param.lo \
	../scr_log.lo \
	../scr_copy_xor.lo \
	../scr_halt.lo \
	../scr_err_serial.lo \
	../scr_config_serial.lo

libscrcmds_la_SOURCES = $(scr_sources)
libscrcmds_la_CFLAGS = $(INCLUDES)
libscrcmds_la_LIBADD = $(MYSQL_LDFLAGS) $(MYSQL_LIBS) $(TV_LIBS) -lz

all-local: libscrcmds.la

clean-local:
	libtool --mode=clean /bin/rm -rf libscrcmds.la $(scr_objs)

# install source code with package
install-exec-local:
	mkdir -p $(DESTDIR)$(prefix)/src/bin
	libtool --mode=install cp *.c $(DESTDIR)$(prefix)/src/bin
	libtool --mode=install cp libscrcmds.la $(DESTDIR)$(prefix)/lib

# uninstall source code with package
uninstall-local:
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/src/bin/scr*
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/lib/libscr*
