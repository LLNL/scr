SUBDIRS = man scripts src doc examples

EXTRA_DIST = DISCLAIMER META README LICENSE.TXT scr.conf scr.mysql deps.txt

install-data-local:
	-$(top_srcdir)/config/install-sh -m 755 -d \
		$(DESTDIR)$(prefix)
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/README \
		$(DESTDIR)$(prefix)

uninstall-local:
	rm -f $(DESTDIR)$(prefix)/README
