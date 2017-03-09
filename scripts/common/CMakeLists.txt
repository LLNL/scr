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

## Configure & install .pm files
CONFIGURE_FILE(scr_param.pm.in scr_param.pm @ONLY)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/scr_param.pm.in PROPERTIES GENERATED FALSE)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/scr_param.pm PROPERTIES GENERATED TRUE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/scr_param.pm DESTINATION ${CMAKE_INSTALL_DATADIR}/scr)
INSTALL(FILES scr_hostlist.pm DESTINATION ${CMAKE_INSTALL_DATADIR}/scr)
