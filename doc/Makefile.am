EXTRA_DIST = \
	scr.pdf

install-data-local:
	-$(top_srcdir)/config/install-sh -m 755 -d \
		$(DESTDIR)$(prefix)/doc
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/scr.pdf \
		$(DESTDIR)$(prefix)/doc

uninstall-local:
	rm -f $(DESTDIR)$(prefix)/doc/scr.pdf
