# Header files need not be enumerated
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_BINARY_DIR})

###########
# LIB SCR #
###########

# Install header files
LIST(APPEND libscr_install_headers
	scr.h
	scrf.h
)
INSTALL(FILES ${libscr_install_headers} DESTINATION include)

## CLI should build without MPI dependence
LIST(APPEND libscr_noMPI_scrs
	scr_compress.c
	scr_config.c
	scr_config_serial.c
	scr_dataset.c
	scr_env.c
	scr_err_serial.c
	scr_filemap.c
	scr_halt.c
	scr_hash.c
	scr_hash_util.c
	scr_index_api.c
	scr_io.c
	scr_log.c
	scr_meta.c
	scr_param.c
	scr_path.c
	scr_util.c
	tv_data_display.c
)

LIST(APPEND libscr_srcs
	scr.c
	scr_cache.c
	scr_cache_rebuild.c
	scr_config_mpi.c
	scr_dataset.c
	scr_err_mpi.c
	scr_fetch.c
	scr_flush.c
	scr_flush_file_mpi.c
	scr_flush_sync.c
	scr_globals.c
	scr_groupdesc.c
	scr_hash.c
	scr_hash_mpi.c
	scr_path_mpi.c
	scr_reddesc.c
	scr_reddesc_apply.c
	scr_reddesc_recover.c
	scr_split.c
	scr_storedesc.c
	scr_summary.c
	scr_util_mpi.c
)

# Add vendor specific code based on SCR_ASYNC_API

IF(${SCR_ASYNC_API} STREQUAL "Cray_DataWarp")
	LIST(APPEND libscr_srcs scr_flush_async_cray_xt.c)
ENDIF(${SCR_ASYNC_API} STREQUAL "Cray_DataWarp")

IF(${SCR_ASYNC_API} STREQUAL "Intel_CPPR")
	LIST(APPEND libscr_srcs scr_flush_async_intel.c)
ENDIF(${SCR_ASYNC_API} STREQUAL "Intel_CPPR")


# SCR Library
ADD_LIBRARY(scr ${libscr_srcs} ${libscr_noMPI_scrs})
TARGET_LINK_LIBRARIES(scr ${SCR_EXTERNAL_LIBS})
INSTALL(TARGETS scr DESTINATION lib)

# Non-MPI library for CLI
ADD_LIBRARY(scr_base ${libscr_noMPI_scrs})
TARGET_LINK_LIBRARIES(scr_base scr)

# Fortran
IF(ENABLE_FORTRAN)
	ADD_LIBRARY(scrf srcf.c ${libscr_srcs} ${libscr_noMPI_scrs})
	TARGET_LINK_LIBRARIES(scrf scr)
ENDIF(ENABLE_FORTRAN)

###########
# CLI SCR #
###########

# Individual binaries generated from a single .c file
LIST(APPEND cliscr_c_bins
	scr_copy
	scr_crc32
	scr_flush_file
	scr_halt_cntl
	scr_index
	scr_inspect_cache
	scr_log_event
	scr_log_transfer
	scr_nodes_file
	scr_print_hash_file
	scr_rebuild_xor
	scr_retries_halt
	scr_transfer
)

# Build and install C binaries
FOREACH(bin IN ITEMS ${cliscr_c_bins})
	ADD_EXECUTABLE(${bin} ${bin}.c)
	TARGET_LINK_LIBRARIES(${bin} SCR_base z)
	INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${bin} DESTINATION bin)
ENDFOREACH(bin IN ITEMS ${cliscr_c_bins})
