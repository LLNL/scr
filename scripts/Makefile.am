SUBDIRS = common $(MACHINE_NAME)
dist_bin_SCRIPTS = \
	common/scr_prefix \
	common/scr_list_dir \
	common/scr_check_node \
	common/scr_glob_hosts \
	common/scr_postrun \
	common/scr_prerun \
	common/scr_test_datemanip \
	common/scr_test_runtime \
        common/scr_watchdog \
	TLCC/scr_inspect \
	TLCC/scr_list_down_nodes \
	TLCC/scr_scavenge \
	TLCC/scr_run \
	TLCC/scr_env \
	TLCC/scr_halt \
        TLCC/scr_kill_jobstep \
        TLCC/scr_get_jobstep_id \
	cray_xt/scr_inspect \
	cray_xt/scr_list_down_nodes \
	cray_xt/scr_scavenge \
	cray_xt/scr_run \
	cray_xt/scr_env \
	cray_xt/scr_halt \
        cray_xt/scr_kill_jobstep \
        cray_xt/scr_get_jobstep_id 



dist_pkgdata_DATA = common/scr_param.pm common/scr_hostlist.pm

install-exec-local:
	rm -f $(DESTDIR)$(bindir)/scr_run
