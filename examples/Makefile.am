files_to_be_installed = \
	test_common.c \
	test_common.h \
	test_api.c \
	test_api_multiple.c \
	test_interpose.c \
	test_interpose_multiple.c \
	test_ckpt.C \
	test_ckpt.F \
	scr.moab \
	scr_interpose.moab

EXTRA_DIST = makefile.examples $(files_to_be_installed)

install-data-local:
	-$(top_srcdir)/config/install-sh -m 755 -d \
		$(DESTDIR)$(prefix)/examples
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/makefile.examples \
		$(DESTDIR)$(prefix)/examples/makefile
	for f in $(files_to_be_installed) ; do \
		$(top_srcdir)/config/install-sh -m 644 $(srcdir)/$$f \
			$(DESTDIR)$(prefix)/examples ; done

uninstall-local:
	rm -f $(DESTDIR)$(prefix)/examples/*
