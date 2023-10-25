SET(common_bins
	scr_postrun
	scr_prerun
)

SET(common_libexec
	scr_prefix
	scr_list_dir
	scr_check_node
	scr_glob_hosts
	scr_poststage
	scr_test_datemanip
	scr_test_runtime
	scr_watchdog
)

#FOREACH(bin IN ITEMS ${common_bins})
#	SCR_INSTALL_BIN(${bin})
#ENDFOREACH(bin IN ITEMS ${common_bins})
#
#FOREACH(bin IN ITEMS ${common_libexec})
#	SCR_INSTALL_LIBEXEC(${bin})
#ENDFOREACH(bin IN ITEMS ${common_libexec})
#
### Configure & install .pm files
#CONFIGURE_FILE(scr_param.pm.in scr_param.pm @ONLY)
#SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/scr_param.pm PROPERTIES GENERATED TRUE)
#INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/scr_param.pm DESTINATION ${CMAKE_INSTALL_DATADIR}/scr)
#INSTALL(FILES scr_hostlist.pm DESTINATION ${CMAKE_INSTALL_DATADIR}/scr)

## Install python files
INSTALL(FILES scrlog.py DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})
INSTALL(FILES scr_ckpt_interval.py DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})
