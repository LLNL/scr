examplesdir = $(pkgdatadir)/examples
dist_examples_DATA = \
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

EXTRA_DIST = makefile.examples

# Use -hook instead of -local b/c we need to do this AFTER the main install data rule.
install-data-hook:
	$(INSTALL_DATA) makefile.examples $(DESTDIR)$(examplesdir)/makefile

uninstall-local:
	-rm -f $(DESTDIR)$(examplesdir)/makefile
