SET(common_bins
	scr_prefix
	scr_list_dir
	scr_check_node
	scr_glob_hosts
	scr_postrun
	scr_prerun
	scr_test_datemanip
	scr_test_runtime
	scr_watchdog
)

FOREACH(bin IN ITEMS ${common_bins})
	SCR_INSTALL_BIN(${bin})
ENDFOREACH(bin IN ITEMS ${common_bins})
