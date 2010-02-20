MPICC     = @MPICC@

AM_CFLAGS = -I$(top_srcdir)/src

include_HEADERS = scr.h

bin_PROGRAMS = \
	scr_inspect_cache \
	scr_copy \
	scr_rebuild_xor \
	scr_halt_cntl \
	scr_crc32 \
	scr_print_hash_file \
	scr_log_event \
	scr_log_transfer \
	scr_transfer

#lib_LTLIBRARIES = \
#	libscr.la \
#	libscr_interpose.la

scr_headers = \
	scr.h \
	scr_err.h \
	scr_io.h \
	scr_util.h \
	scr_hash.h \
	tv_data_display.h \
	scr_filemap.h \
	scr_meta.h \
	scr_config.h \
	scr_param.h \
	scr_log.h \
	scr_copy_xor.h \
	scr_halt.h

scr_srcs = \
	scr.c \
	scr_io.c \
	scr_util.c \
	scr_hash.c \
	tv_data_display.c \
	scr_filemap.c \
	scr_meta.c \
	scr_config.c \
	scr_param.c \
	scr_log.c \
	scr_copy_xor.c \
	scr_halt.c

scr_objs = \
	scr.lo \
	scr_io.lo \
	scr_util.lo \
	scr_hash.lo \
	tv_data_display.lo \
	scr_filemap.lo \
	scr_meta.lo \
	scr_config.lo \
	scr_param.lo \
	scr_log.lo \
	scr_copy_xor.lo \
	scr_halt.lo

SCR_C_FLAGS = \
	$(DEFS) $(DEFAULT_INCLUDES) $(INCLUDES) \
	$(AM_CPPFLAGS) $(CPPFLAGS) $(AM_CFLAGS) $(CFLAGS) \
	$(YOGRT_CPPFLAGS) \
	$(MYSQL_CPPFLAGS)

scr_io.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_io.c -o $@ $(SCR_C_FLAGS)

scr_util.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_util.c -o $@ $(SCR_C_FLAGS)

scr_hash.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_hash.c -o $@ $(SCR_C_FLAGS)

tv_data_display.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c tv_data_display.c -o $@ $(SCR_C_FLAGS)

scr_filemap.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_filemap.c -o $@ $(SCR_C_FLAGS)

scr_meta.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_meta.c -o $@ $(SCR_C_FLAGS)

scr_config.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_config.c -o $@ $(SCR_C_FLAGS)

scr_param.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_param.c -o $@ $(SCR_C_FLAGS)

scr_log.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_log.c -o $@ $(SCR_C_FLAGS)

scr_copy_xor.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_copy_xor.c -o $@ $(SCR_C_FLAGS)

scr_halt.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(CC) -c scr_halt.c -o $@ $(SCR_C_FLAGS)

scr.lo: $(scr_headers) $(scr_sources)
	libtool --tag=CC --mode=compile $(MPICC) -c scr.c -o $@ $(SCR_C_FLAGS)

scr_interpose.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(MPICC) -c scr_interpose.c -o $@ $(SCR_C_FLAGS)

#libscr.a: libscr.la $(scr_objs)

libscr.la: $(scr_objs)
	libtool --mode=link --tag=CC $(MPICC) $(SCR_C_FLAGS) -o $@ \
	$(scr_objs) \
	$(YOGRT_LDFLAGS) $(YOGRT_LIBS) $(MYSQL_LDFLAGS) $(MYSQL_LIBS) $(TV_LIBS) -lz -rpath $(DESTDIR)$(libdir)

#libscr_interpose.a: libscr_interpose.la $(scr_objs) scr_interpose.lo

libscr_interpose.la: $(scr_objs) scr_interpose.lo
	libtool --mode=link --tag=CC $(MPICC) $(SCR_C_FLAGS) -o $@ \
	$(scr_objs) scr_interpose.lo \
	$(YOGRT_LDFLAGS) $(YOGRT_LIBS) $(MYSQL_LDFLAGS) $(MYSQL_LIBS) $(TV_LIBS) -lz -rpath $(DESTDIR)$(libdir)

all-local: libscr.la libscr_interpose.la
	
clean-local:
	libtool --mode=clean /bin/rm -rf libscr.la libscr_interpose.la

install-exec-local:
	mkdir -p $(DESTDIR)$(prefix)/include
	mkdir -p $(DESTDIR)$(prefix)/lib
	mkdir -p $(DESTDIR)$(prefix)/src
	cp scr.h $(DESTDIR)$(prefix)/include/.
	libtool --mode=install cp libscr.la $(DESTDIR)$(prefix)/lib
	libtool --mode=install cp libscr_interpose.la $(DESTDIR)$(prefix)/lib
	cp ../config/config.h $(DESTDIR)$(prefix)/src
	cp *.c *.h $(DESTDIR)$(prefix)/src

# TODO: there must be a better way to do this uninstall, but I don't know how
uninstall-local:
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/lib/libscr*
	rm -rf $(DESTDIR)$(prefix)/src/*

#nodist_libscr_SOURCES = libscr.c
#nodist_libscr_interpose_SOURCES = libscr.c

#lib_LTLIBRARIES = libscr.la 
#libscr_la_SOURCES = \
#	scr.h \
#	scr_err.h \
#	scr_io.c          scr_io.h \
#	scr_util.c        scr_util.h \
#	scr_hash.c        scr_hash.h \
#	tv_data_display.c tv_data_display.h \
#	scr_filemap.c     scr_filemap.h \
#	scr_meta.c        scr_meta.h \
#	scr_config.c      scr_config.h \
#	scr_param.c       scr_param.h \
#	scr_log.c         scr_log.h \
#	scr_copy_xor.c    scr_copy_xor.h \
#	scr_halt.c        scr_halt.h \
#	scr.c
#libscr_la_LIBADD = $(YOGRT_LDFLAGS) $(YOGRT_LIBS) $(MYSQL_LDFLAGS) $(MYSQL_LIBS) $(TV_LIBS) -lz
#libscr_la_CFLAGS = $(AM_CFLAGS) $(YOGRT_CPPFLAGS) $(MYSQL_CPPFLAGS)

#lib_LTLIBRARIES += libscr_interpose.la
#libscr_interpose_la_SOURCES = \
#	scr.h \
#	scr_err.h \
#	scr_io.c          scr_io.h \
#	scr_util.c        scr_util.h \
#	scr_hash.c        scr_hash.h \
#	tv_data_display.c tv_data_display.h \
#	scr_filemap.c     scr_filemap.h \
#	scr_meta.c        scr_meta.h \
#	scr_config.c      scr_config.h \
#	scr_param.c       scr_param.h \
#	scr_log.c         scr_log.h \
#	scr_copy_xor.c    scr_copy_xor.h \
#	scr_halt.c        scr_halt.h \
#	scr.c \
#	scr_interpose.c
#libscr_interpose_la_LIBADD = $(YOGRT_LDFLAGS) $(YOGRT_LIBS) $(MYSQL_LDFLAGS) $(MYSQL_LIBS) -lz
#libscr_interpose_la_CFLAGS = $(AM_CFLAGS) $(YOGRT_CPPFLAGS)

scr_copy_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_meta.c        scr_meta.h \
	scr_filemap.c     scr_filemap.h \
	scr_hash.c        scr_hash.h \
	tv_data_display.c tv_data_display.h \
	scr_copy.c
scr_copy_LDADD = -lz
scr_copy_CFLAGS = $(AM_CFLAGS)

scr_inspect_cache_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_meta.c        scr_meta.h \
	scr_filemap.c     scr_filemap.h \
	scr_hash.c        scr_hash.h \
	tv_data_display.c tv_data_display.h \
	scr_inspect_cache.c
scr_inspect_cache_LDADD = -lz
scr_inspect_cache_CFLAGS = $(AM_CFLAGS)

scr_rebuild_xor_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_meta.c        scr_meta.h \
	scr_filemap.c     scr_filemap.h \
	scr_hash.c        scr_hash.h \
	tv_data_display.c tv_data_display.h \
	scr_copy_xor.c    scr_copy_xor.h \
	scr_rebuild_xor.c
scr_rebuild_xor_LDADD = -lz
scr_rebuild_xor_CFLAGS = $(AM_CFLAGS)

scr_halt_cntl_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_hash.c        scr_hash.h \
	tv_data_display.c tv_data_display.h \
	scr_halt.c        scr_halt.h \
	scr_halt_cntl.c
scr_halt_cntl_LDADD = -lz
scr_halt_cntl_CFLAGS = $(AM_CFLAGS)

scr_crc32_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_crc32.c
scr_crc32_LDADD = -lz
scr_crc32_CFLAGS = $(AM_CFLAGS)

scr_print_hash_file_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_hash.c        scr_hash.h \
	tv_data_display.c tv_data_display.h \
	scr_print_hash_file.c
scr_print_hash_file_LDADD = -lz
scr_print_hash_file_CFLAGS = $(AM_CFLAGS)

scr_log_event_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_hash.c        scr_hash.h \
	tv_data_display.c tv_data_display.h \
	scr_config.c      scr_config.h      scr_config_serial.c \
	scr_param.c       scr_param.h \
	scr_log.c         scr_log.h \
	scr_log_event.c
scr_log_event_LDADD = -lz $(MYSQL_LDFLAGS) $(MYSQL_LIBS)
scr_log_event_CFLAGS = $(AM_CFLAGS)

scr_log_transfer_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_hash.c        scr_hash.h \
	tv_data_display.c tv_data_display.h \
	scr_config.c      scr_config.h      scr_config_serial.c \
	scr_param.c       scr_param.h \
	scr_log.c         scr_log.h \
	scr_log_transfer.c
scr_log_transfer_LDADD = -lz $(MYSQL_LDFLAGS) $(MYSQL_LIBS)
scr_log_transfer_CFLAGS = $(AM_CFLAGS)

scr_transfer_SOURCES = \
	scr.h \
	scr_err_serial.c  scr_err.h \
	scr_io.c          scr_io.h \
	scr_util.c        scr_util.h \
	scr_hash.c        scr_hash.h \
	tv_data_display.c tv_data_display.h \
	scr_config.c      scr_config.h      scr_config_serial.c \
	scr_param.c       scr_param.h \
	scr_transfer.c
scr_transfer_LDADD = -lz
scr_transfer_CFLAGS = $(AM_CFLAGS)

EXTRA_DIST = queue.h $(scr_headers) $(scr_srcs) scr_interpose.c
