AM_CFLAGS = -I$(top_srcdir)/src -I$(top_srcdir)/config

SUBDIRS = . lib bin

include_HEADERS = scr.h scrf.h

scr_headers = \
	scr_conf.h \
	scr.h \
	scrf.h \
	scr_err.h \
	scr_io.h \
	scr_util.h \
	scr_hash.h \
	scr_hash_util.h \
	tv_data_display.h \
	scr_filemap.h \
	scr_index_api.h \
	scr_meta.h \
	scr_config.h \
	scr_param.h \
	scr_log.h \
	scr_copy_xor.h \
	scr_halt.h

scr_sources = \
	scr_io.c \
	scr_util.c \
	scr_hash.c \
	scr_hash_util.c \
	tv_data_display.c \
	scr_filemap.c \
	scr_index_api.c \
	scr_meta.c \
	scr_config.c \
	scr_param.c \
	scr_log.c \
	scr_copy_xor.c \
	scr_halt.c

scr_objs = \
	scr_io.lo \
	scr_util.lo \
	scr_hash.lo \
	scr_hash_util.lo \
	tv_data_display.lo \
	scr_filemap.lo \
	scr_index_api.lo \
	scr_meta.lo \
	scr_config.lo \
	scr_param.lo \
	scr_log.lo \
	scr_copy_xor.lo \
	scr_halt.lo

all-local:
	
clean-local:
	libtool --mode=clean /bin/rm -rf $(scr_objs)

install-exec-local:
	mkdir -p $(DESTDIR)$(prefix)/include
	mkdir -p $(DESTDIR)$(prefix)/src
	libtool --mode=install cp scr.h $(DESTDIR)$(prefix)/include/.
	libtool --mode=install cp scrf.h $(DESTDIR)$(prefix)/include/.
	libtool --mode=install cp ../config/config.h $(DESTDIR)$(prefix)/src
	libtool --mode=install cp *.c *.h $(DESTDIR)$(prefix)/src

# TODO: there must be a better way to do this uninstall, but I don't know how
uninstall-local:
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/src/scr*
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/src/{queue,config}.h
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/src/tv_data_display.{c,h}
	libtool --mode=uninstall /bin/rm -rf $(DESTDIR)$(prefix)/lib/libscr*

EXTRA_DIST = queue.h $(scr_headers) $(scr_sources)
