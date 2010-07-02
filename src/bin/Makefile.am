MPICC     = @MPICC@

AM_CFLAGS = -I$(top_srcdir)/src

include_HEADERS = ../scr.h

bin_PROGRAMS = \
	scr_inspect_cache \
	scr_copy \
	scr_rebuild_xor \
	scr_halt_cntl \
	scr_crc32 \
	scr_print_hash_file \
	scr_log_event \
	scr_log_transfer \
	scr_transfer \
	scr_index

scr_copy_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_meta.c        ../scr_meta.h \
	../scr_filemap.c     ../scr_filemap.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	scr_copy.c
scr_copy_LDADD = -lz
scr_copy_CFLAGS = $(AM_CFLAGS)

scr_inspect_cache_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_meta.c        ../scr_meta.h \
	../scr_filemap.c     ../scr_filemap.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	scr_inspect_cache.c
scr_inspect_cache_LDADD = -lz
scr_inspect_cache_CFLAGS = $(AM_CFLAGS)

scr_rebuild_xor_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_meta.c        ../scr_meta.h \
	../scr_filemap.c     ../scr_filemap.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	../scr_copy_xor.c    ../scr_copy_xor.h \
	scr_rebuild_xor.c
scr_rebuild_xor_LDADD = -lz
scr_rebuild_xor_CFLAGS = $(AM_CFLAGS)

scr_halt_cntl_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	../scr_halt.c        ../scr_halt.h \
	scr_halt_cntl.c
scr_halt_cntl_LDADD = -lz
scr_halt_cntl_CFLAGS = $(AM_CFLAGS)

scr_crc32_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	scr_crc32.c
scr_crc32_LDADD = -lz
scr_crc32_CFLAGS = $(AM_CFLAGS)

scr_print_hash_file_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	scr_print_hash_file.c
scr_print_hash_file_LDADD = -lz
scr_print_hash_file_CFLAGS = $(AM_CFLAGS)

scr_log_event_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	../scr_config.c      ../scr_config.h      ../scr_config_serial.c \
	../scr_param.c       ../scr_param.h \
	../scr_log.c         ../scr_log.h \
	scr_log_event.c
scr_log_event_LDADD = -lz $(MYSQL_LDFLAGS) $(MYSQL_LIBS)
scr_log_event_CFLAGS = $(AM_CFLAGS)

scr_log_transfer_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	../scr_config.c      ../scr_config.h      ../scr_config_serial.c \
	../scr_param.c       ../scr_param.h \
	../scr_log.c         ../scr_log.h \
	scr_log_transfer.c
scr_log_transfer_LDADD = -lz $(MYSQL_LDFLAGS) $(MYSQL_LIBS)
scr_log_transfer_CFLAGS = $(AM_CFLAGS)

scr_transfer_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_util.c        ../scr_util.h \
	../scr_hash.c        ../scr_hash.h \
	../tv_data_display.c ../tv_data_display.h \
	../scr_config.c      ../scr_config.h      ../scr_config_serial.c \
	../scr_param.c       ../scr_param.h \
	scr_transfer.c
scr_transfer_LDADD = -lz
scr_transfer_CFLAGS = $(AM_CFLAGS)

scr_index_SOURCES = \
	../scr_conf.h \
	../scr.h \
	../scr_err_serial.c  ../scr_err.h \
	../scr_io.c          ../scr_io.h \
	../scr_meta.c        ../scr_meta.h \
	../scr_filemap.c     ../scr_filemap.h \
	../scr_hash.c        ../scr_hash.h \
	../scr_index_api.c   ../scr_index_api.h \
	../tv_data_display.c ../tv_data_display.h \
	scr_index.c
scr_index_LDADD = -lz
scr_index_CFLAGS = $(AM_CFLAGS)

# install source code with package
install-exec-local:
	mkdir -p $(DESTDIR)$(prefix)/src/bin
	libtool --mode=install cp *.c $(DESTDIR)$(prefix)/src/bin

# uninstall source code with package
uninstall-local:
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/src/bin/scr*
