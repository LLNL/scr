MPICC     = @MPICC@

AM_CFLAGS = -I$(top_srcdir)/src -I$(top_srcdir)/config

#include_HEADERS = ../scr.h

scr_headers = \
	../scr_conf.h \
	../scr.h \
	../scrf.h \
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

scr_sources = \
	scr.c \
	scrf.c \
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
	../scr_halt.c

scr_objs = \
	scr.lo \
	scrf.lo \
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
	../scr_halt.lo

SCR_C_FLAGS = \
	$(DEFS) \
	$(DEFAULT_INCLUDES) $(INCLUDES) \
	$(AM_CPPFLAGS) $(CPPFLAGS) \
	$(AM_CFLAGS) $(CFLAGS) \
	$(YOGRT_CPPFLAGS) \
	$(MYSQL_CPPFLAGS)

scr.lo: $(scr_headers) $(scr_sources)
	libtool --tag=CC --mode=compile $(MPICC) -c scr.c -o $@ $(SCR_C_FLAGS)

scrf.lo: $(scr_headers) $(scr_sources)
	libtool --tag=CC --mode=compile $(MPICC) -c scrf.c -o $@ $(SCR_C_FLAGS)

scr_interpose.lo: $(scr_headers) $(scr_sources)
	libtool --mode=compile --tag=CC $(MPICC) -c scr_interpose.c -o $@ $(SCR_C_FLAGS)

libscr.la: $(scr_objs)
	libtool --mode=link --tag=CC $(MPICC) $(SCR_C_FLAGS) -o $@ \
	$(scr_objs) \
	$(YOGRT_LDFLAGS) $(YOGRT_LIBS) $(MYSQL_LDFLAGS) $(MYSQL_LIBS) $(TV_LIBS) -lz -rpath $(DESTDIR)$(libdir)

libscr_interpose.la: $(scr_objs) scr_interpose.lo
	libtool --mode=link --tag=CC $(MPICC) $(SCR_C_FLAGS) -o $@ \
	$(scr_objs) scr_interpose.lo \
	$(YOGRT_LDFLAGS) $(YOGRT_LIBS) $(MYSQL_LDFLAGS) $(MYSQL_LIBS) $(TV_LIBS) -lz -rpath $(DESTDIR)$(libdir)

all-local: libscr.la libscr_interpose.la
	
clean-local:
	libtool --mode=clean /bin/rm -rf libscr.la libscr_interpose.la *.o

install-exec-local:
	mkdir -p $(DESTDIR)$(prefix)/lib
	mkdir -p $(DESTDIR)$(prefix)/src/lib
	libtool --mode=install cp libscr.la $(DESTDIR)$(prefix)/lib
	libtool --mode=install cp libscr_interpose.la $(DESTDIR)$(prefix)/lib
	libtool --mode=install cp *.c $(DESTDIR)$(prefix)/src/lib

# TODO: there must be a better way to do this uninstall, but I don't know how
uninstall-local:
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/lib/libscr*
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/src/lib/scr*

EXTRA_DIST = scr.c scrf.c scr_interpose.c
