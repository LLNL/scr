SUBDIRS = common $(MACHINE_NAME)
dist_bin_SCRIPTS = \
	common/scr_cntl_dir \
	common/scr_check_node \
	common/scr_glob_hosts \
	common/scr_postrun \
	common/scr_prerun \
	common/scr_test_datemanip \
	common/scr_test_runtime \
        common/scr_watchdog \
	$(MACHINE_NAME)/scr_inspect \
	$(MACHINE_NAME)/scr_list_down_nodes \
	$(MACHINE_NAME)/scr_scavenge \
	$(MACHINE_NAME)/scr_run \
	$(MACHINE_NAME)/scr_env \
	$(MACHINE_NAME)/scr_halt \
        $(MACHINE_NAME)/scr_kill_jobstep \
        $(MACHINE_NAME)/scr_get_jobstep_id 


dist_pkgdata_DATA = common/scr_param.pm common/scr_hostlist.pm

install-exec-local:
	rm -f $(DESTDIR)$(bindir)/scr_run
