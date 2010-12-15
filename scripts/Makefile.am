bin_SCRIPTS = \
	scr_cntl_dir \
	scr_check_node \
	scr_env \
	scr_inspect \
	scr_flush \
	scr_glob_hosts \
	scr_halt \
	scr_list_down_nodes \
	scr_postrun \
	scr_prerun \
	scr_srun \
	scr_test_datemanip \
	scr_test_runtime

EXTRA_DIST = \
	scr_cntl_dir \
	scr_check_node \
	scr_env \
	scr_inspect \
	scr_flush \
	scr_glob_hosts \
	scr_halt \
	scr_list_down_nodes \
	scr_postrun \
	scr_prerun \
	scr_srun \
	scr_test_datemanip \
	scr_test_runtime

install-data-local:
	-$(top_srcdir)/config/install-sh -m 755 -d \
		$(DESTDIR)$(datadir)/scr
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/scr_param.pm \
		$(DESTDIR)$(datadir)/scr
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/scr_hostlist.pm \
		$(DESTDIR)$(datadir)/scr

EXTRA_DIST += scr_param.pm scr_hostlist.pm
