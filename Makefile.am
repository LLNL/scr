ACLOCAL_AMFLAGS = -I m4

SUBDIRS = man scripts src doc examples

EXTRA_DIST = DISCLAIMER META NEWS README LICENSE.TXT scr.conf scr.mysql

install-data-local:
	-$(top_srcdir)/config/install-sh -m 755 -d \
		$(DESTDIR)$(prefix)
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/README \
		$(DESTDIR)$(prefix)

uninstall-local:
	rm -f $(DESTDIR)$(prefix)/README
