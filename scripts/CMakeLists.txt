
# Function to configure binaries (file.in -> file) and install into bin directory
# Called from the subdirectories added below
FUNCTION(SCR_INSTALL_BIN file)
	CONFIGURE_FILE(${file}.in ${file} @ONLY)
	SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/${file}.in PROPERTIES GENERATED FALSE)
	SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/${file} PROPERTIES GENERATED TRUE)

	INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${file} DESTINATION bin)
ENDFUNCTION(SCR_INSTALL_BIN file)


# Common Binaries
ADD_SUBDIRECTORY(common)


# Pick the Resource Manager
SET(cliscr_dir_name "")

IF(${SCR_RESOURCE_MANAGER} STREQUAL "SLURM")
	SET(cliscr_dir_name "TLCC")
ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "SLURM")

IF(${SCR_RESOURCE_MANAGER} STREQUAL "APRUN")
	SET(cliscr_dir_name "cray_xt")
ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "APRUN")

IF(${SCR_RESOURCE_MANAGER} STREQUAL "PMIx")
	SET(cliscr_dir_name "PMIX")
ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "PMIx")

IF(${SCR_RESOURCE_MANAGER} STREQUAL "LSF")
	SET(cliscr_dir_name "LSF")
ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "LSF")


IF(${SCR_RESOURCE_MANAGER} STREQUAL "None")
	MESSAGE("No resource manager selected. Some of the SCR command line interface will not be built.")
ELSE(${SCR_RESOURCE_MANAGER} STREQUAL "None")
	# Each of the following must be implemented for all resource managers
	LIST(APPEND resource_manager_bins
		scr_run
		scr_env
		scr_scavenge
		scr_inspect
		scr_list_down_nodes
		scr_halt
		scr_kill_jobstep
		scr_get_jobstep_id
	)

	FOREACH(bin IN ITEMS ${resource_manager_bins})
		SCR_INSTALL_BIN(${cliscr_dir_name}/${bin})
	ENDFOREACH(bin IN ITEMS ${resource_manager_bins})
ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "None")


