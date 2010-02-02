bin_SCRIPTS = \
	scr_cntl_dir \
	scr_check_complete \
	scr_check_node \
	scr_env \
	scr_inspect \
	scr_flush \
	scr_flush_file \
	scr_glob_hosts \
	scr_halt \
	scr_list_down_nodes \
	scr_postrun \
	scr_prerun \
	scr_retries_halt \
	scr_srun

EXTRA_DIST = \
	scr_cntl_dir \
	scr_check_complete \
	scr_check_node \
	scr_env \
	scr_inspect \
	scr_flush \
	scr_flush_file \
	scr_glob_hosts \
	scr_halt \
	scr_list_down_nodes \
	scr_postrun \
	scr_prerun \
	scr_retries_halt \
	scr_srun

install-data-local:
	-$(top_srcdir)/config/install-sh -m 755 -d \
		$(DESTDIR)$(datadir)/scr
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/scr_meta.pm \
		$(DESTDIR)$(datadir)/scr
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/scr_hash.pm \
		$(DESTDIR)$(datadir)/scr
	$(top_srcdir)/config/install-sh -m 644 $(srcdir)/scr_param.pm \
		$(DESTDIR)$(datadir)/scr

EXTRA_DIST += scr_meta.pm scr_hash.pm scr_param.pm
