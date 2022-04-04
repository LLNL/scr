PROJECT(SCR_TOP)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# Find Packages & Files
LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/scr/cmake")
LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/axl/cmake")

# process SCR Cmake options
INCLUDE(SCR_OPTIONS)

# forward file lock setting on to kvtree compile
SET(KVTREE_FILE_LOCK ${SCR_FILE_LOCK})

#LIST(APPEND CMAKE_OPTS "-DZLIB_ROOT=${ZLIB_ROOT}")

## HEADERS
INCLUDE(CheckIncludeFile)

# check for byteswap file for kvtree
CHECK_INCLUDE_FILE(byteswap.h HAVE_BYTESWAP_H)

# search for common dependent libs and generate config file
INCLUDE(SCR_DEPENDENCIES)

## IBM Burst Buffer API
IF(ENABLE_IBM_BBAPI)
    FIND_PACKAGE(BBAPI)
    IF(BBAPI_FOUND)
        SET(HAVE_BBAPI TRUE)

        SET(ENABLE_BBAPI_FALLBACK OFF CACHE BOOL "Fallback to a different transfer type if BBAPI not supported")
        IF(${ENABLE_BBAPI_FALLBACK})
            SET(HAVE_BBAPI_FALLBACK TRUE)
        ENDIF(${ENABLE_BBAPI_FALLBACK})

        INCLUDE_DIRECTORIES(${BBAPI_INCLUDE_DIRS})
        LIST(APPEND SCR_EXTERNAL_LIBS ${BBAPI_LIBRARIES})
        LIST(APPEND SCR_EXTERNAL_SERIAL_LIBS ${BBAPI_LIBRARIES})
        LIST(APPEND SCR_LINK_LINE " -L${WITH_BBAPI_PREFIX}/lib -lbbAPI")
    ENDIF(BBAPI_FOUND)
ENDIF(ENABLE_IBM_BBAPI)

INCLUDE(GNUInstallDirs)
## Use X_ variable names for CLI scripts
## could use CMAKE_INSTALL_FULL_ names instead
SET(X_BINDIR ${CMAKE_INSTALL_FULL_BINDIR} CACHE INTERNAL "bin")
SET(X_DATADIR ${CMAKE_INSTALL_FULL_DATADIR} CACHE INTERNAL "share")
SET(X_INCLUDEDIR ${CMAKE_INSTALL_FULL_INCLUDEDIR} CACHE INTERNAL "include")
SET(X_LIBDIR ${CMAKE_INSTALL_FULL_LIBDIR} CACHE INTERNAL "lib")

# Generate config.h with all our build #defs
CONFIGURE_FILE(${PROJECT_SOURCE_DIR}/cmake/config.h.in ${PROJECT_BINARY_DIR}/config.h)
SET_SOURCE_FILES_PROPERTIES(${PROJECT_BINARY_DIR}/config.h PROPERTIES GENERATED TRUE)

INCLUDE_DIRECTORIES(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${PROJECT_BINARY_DIR}
    lwgrp/src
    dtcmp/src
    kvtree/src
    spath/src
    rankstr/src
    axl/src
    shuffile/src
    redset/src
    er/src
    scr/src
)

INCLUDE(SCR_ADD_TEST)
IF(ENABLE_TESTS)
    INCLUDE(CTest)
ENDIF(ENABLE_TESTS)

ADD_SUBDIRECTORY(scr/man)
ADD_SUBDIRECTORY(scr/scripts)
ADD_SUBDIRECTORY(scr/python)
IF(ENABLE_EXAMPLES)
    ADD_SUBDIRECTORY(scr/examples)
ENDIF()

#
# TODO: Add proper lwgrp/src/CMakeLists.txt file so that the following
# lists of source may be replaced with add_subdirectory
#
LIST(APPEND lwgrp_srcs
    lwgrp/src/lwgrp.c
    lwgrp/src/lwgrp_util.c
    lwgrp/src/lwgrp_chain_ops.c
    lwgrp/src/lwgrp_ring_ops.c
    lwgrp/src/lwgrp_logchain_ops.c
    lwgrp/src/lwgrp_logring_ops.c
    lwgrp/src/lwgrp_comm.c
    lwgrp/src/lwgrp_comm_split.c
)

LIST(APPEND dtcmp_srcs
    dtcmp/src/dtcmp.c
    dtcmp/src/dtcmp_util.c
    dtcmp/src/dtcmp_ops.c
    dtcmp/src/dtcmp_uniqify.c
    dtcmp/src/dtcmp_search_binary.c
    dtcmp/src/dtcmp_partitionz.c
    dtcmp/src/dtcmp_partitionz_list.c
    dtcmp/src/dtcmp_partition_local.c
    dtcmp/src/dtcmp_merge_2way.c
    dtcmp/src/dtcmp_merge_kway_heap.c
    dtcmp/src/dtcmp_select_local_ends.c
    dtcmp/src/dtcmp_select_local_randpartition.c
    dtcmp/src/dtcmp_selectv_rand.c
    dtcmp/src/dtcmp_selectv_medianofmedians.c
    dtcmp/src/dtcmp_is_sorted.c
    dtcmp/src/dtcmp_sort_local_insertionsort.c
    dtcmp/src/dtcmp_sort_local_randquicksort.c
    dtcmp/src/dtcmp_sort_local_mergesort.c
    dtcmp/src/dtcmp_sort_local_qsort.c
    dtcmp/src/dtcmp_sort_allgather.c
    dtcmp/src/dtcmp_sort_bitonic.c
    dtcmp/src/dtcmp_sort_samplesort.c
    dtcmp/src/dtcmp_sortv_allgather.c
    dtcmp/src/dtcmp_sortv_sortgather_scatter.c
    dtcmp/src/dtcmp_sortv_cheng.c
    dtcmp/src/dtcmp_sortz_samplesort.c
    dtcmp/src/dtcmp_rankv_sort.c
    dtcmp/src/dtcmp_seg_exscan.c
)

LIST(APPEND cliscr_noMPI_srcs
    scr/src/scr_cache_index.c
    scr/src/scr_cache_index_serial.c
    scr/src/scr_config.c
    scr/src/scr_config_serial.c
    scr/src/scr_dataset.c
    scr/src/scr_env.c
    scr/src/scr_err_serial.c
    scr/src/scr_filemap.c
    scr/src/scr_halt.c
    scr/src/scr_index_api.c
    scr/src/scr_io.c
    scr/src/scr_log.c
    scr/src/scr_meta.c
    scr/src/scr_param.c
    scr/src/scr_util.c
    scr/src/scr_rebuild_xor.c
    scr/src/scr_rebuild_partner.c
    scr/src/scr_rebuild_rs.c
    scr/src/scr_flush_nompi.c
)

LIST(APPEND libscr_srcs
    scr/src/scr.c
    scr/src/scr_cache.c
    scr/src/scr_cache_rebuild.c
    scr/src/scr_cache_index.c
    scr/src/scr_cache_index_mpi.c
    scr/src/scr_config.c
    scr/src/scr_config_mpi.c
    scr/src/scr_dataset.c
    scr/src/scr_dataset.c
    scr/src/scr_env.c
    scr/src/scr_err_mpi.c
    scr/src/scr_fetch.c
    scr/src/scr_filemap.c
    scr/src/scr_flush.c
    scr/src/scr_flush_file_mpi.c
    scr/src/scr_flush_sync.c
    scr/src/scr_flush_async.c
    scr/src/scr_globals.c
    scr/src/scr_groupdesc.c
    scr/src/scr_halt.c
    scr/src/scr_index_api.c
    scr/src/scr_io.c
    scr/src/scr_log.c
    scr/src/scr_meta.c
    scr/src/scr_param.c
    scr/src/scr_prefix.c
    scr/src/scr_reddesc.c
    scr/src/scr_storedesc.c
    scr/src/scr_summary.c
    scr/src/scr_util.c
    scr/src/scr_util_mpi.c
    scr/src/axl_mpi.c
    scr/src/scr_flush_nompi.c
)

ADD_SUBDIRECTORY(axl/src EXCLUDE_FROM_ALL)
ADD_SUBDIRECTORY(er/src EXCLUDE_FROM_ALL)
ADD_SUBDIRECTORY(kvtree/src EXCLUDE_FROM_ALL)
ADD_SUBDIRECTORY(rankstr/src EXCLUDE_FROM_ALL)
ADD_SUBDIRECTORY(redset/src EXCLUDE_FROM_ALL)
ADD_SUBDIRECTORY(shuffile/src EXCLUDE_FROM_ALL)
ADD_SUBDIRECTORY(spath/src EXCLUDE_FROM_ALL)

ADD_LIBRARY(scr_o OBJECT
    ${lwgrp_srcs}
    ${dtcmp_srcs}
    ${libscr_srcs}
)

LIST(APPEND scr_tpl_objects
  $<TARGET_OBJECTS:axl_o>
  $<TARGET_OBJECTS:er_o>
  $<TARGET_OBJECTS:kvtree_o>
  $<TARGET_OBJECTS:rankstr_o>
  $<TARGET_OBJECTS:redset_o>
  $<TARGET_OBJECTS:shuffile_o>
  $<TARGET_OBJECTS:spath_o>
)

LIST(APPEND scr_tpl_noMPI_objects
  $<TARGET_OBJECTS:axl_o>
  $<TARGET_OBJECTS:kvtree_noMPI_o>
  $<TARGET_OBJECTS:redset_noMPI_o>
  $<TARGET_OBJECTS:spath_base_o>
)

# Install header files
LIST(APPEND libscr_install_headers
    scr/src/scr.h
)
IF(ENABLE_FORTRAN)
    LIST(APPEND libscr_install_headers scr/src/scrf.h)
ENDIF(ENABLE_FORTRAN)
INSTALL(FILES ${libscr_install_headers} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

IF(BUILD_SHARED_LIBS)
    ADD_LIBRARY(scr SHARED $<TARGET_OBJECTS:scr_o> ${scr_tpl_objects})
    TARGET_LINK_LIBRARIES(scr ${SCR_EXTERNAL_LIBS})
    SET_TARGET_PROPERTIES(scr PROPERTIES OUTPUT_NAME scr CLEAN_DIRECT_OUTPUT 1)
    INSTALL(TARGETS scr DESTINATION ${CMAKE_INSTALL_LIBDIR})
ENDIF()

ADD_LIBRARY(scr-static STATIC $<TARGET_OBJECTS:scr_o> ${scr_tpl_objects})
IF(SCR_LINK_STATIC)
    SET_TARGET_PROPERTIES(scr-static PROPERTIES LINK_SEARCH_START_STATIC 1)
    SET_TARGET_PROPERTIES(scr-static PROPERTIES LINK_SEARCH_END_STATIC 1)
ENDIF(SCR_LINK_STATIC)
TARGET_LINK_LIBRARIES(scr-static ${SCR_EXTERNAL_LIBS})

SET_TARGET_PROPERTIES(scr-static PROPERTIES OUTPUT_NAME scr CLEAN_DIRECT_OUTPUT 1)
INSTALL(TARGETS scr-static DESTINATION ${CMAKE_INSTALL_LIBDIR})

# Fortran
IF(ENABLE_FORTRAN)
    IF(BUILD_SHARED_LIBS)
        ADD_LIBRARY(scrf SHARED scr/src/scrf.c $<TARGET_OBJECTS:scr_o> ${scr_tpl_objects})
        TARGET_LINK_LIBRARIES(scrf ${SCR_EXTERNAL_LIBS})
        SET_TARGET_PROPERTIES(scrf PROPERTIES OUTPUT_NAME scrf CLEAN_DIRECT_OUTPUT 1)
        INSTALL(TARGETS scrf DESTINATION ${CMAKE_INSTALL_LIBDIR})
    ENDIF()

    ADD_LIBRARY(scrf-static STATIC scr/src/scrf.c $<TARGET_OBJECTS:scr_o> ${scr_tpl_objects})
    IF(SCR_LINK_STATIC)
        SET_TARGET_PROPERTIES(scrf-static PROPERTIES LINK_SEARCH_START_STATIC 1)
        SET_TARGET_PROPERTIES(scrf-static PROPERTIES LINK_SEARCH_END_STATIC 1)
    ENDIF(SCR_LINK_STATIC)
    TARGET_LINK_LIBRARIES(scrf-static ${SCR_EXTERNAL_LIBS})

    SET_TARGET_PROPERTIES(scrf-static PROPERTIES OUTPUT_NAME scrf CLEAN_DIRECT_OUTPUT 1)
    INSTALL(TARGETS scrf-static DESTINATION ${CMAKE_INSTALL_LIBDIR})
ENDIF(ENABLE_FORTRAN)

###########
# CLI SCR #
###########

# Non-MPI library for CLI
ADD_LIBRARY(scr_base STATIC
    ${cliscr_noMPI_srcs}
    ${scr_tpl_noMPI_objects}
)
TARGET_LINK_LIBRARIES(scr_base ${SCR_EXTERNAL_SERIAL_LIBS})

# Individual binaries generated from a single .c file
#    scr_inspect_cache
LIST(APPEND cliscr_c_bins
    scr_crc32
    scr_flush_file
    scr_halt_cntl
    scr_log_event
    scr_log_transfer
    scr_nodes_file
    scr_retries_halt
    scr_copy
    scr_index
    scr_rebuild_xor
    scr_rebuild_partner
    scr_rebuild_rs
    scr_print
)

# CLI binaries that require full SCR library
#LIST(APPEND cliscr_scr_bins
#    scr_have_restart
#)

# Build and install C binaries
FOREACH(bin IN ITEMS ${cliscr_c_bins})
    ADD_EXECUTABLE(${bin} scr/src/${bin}.c)
    TARGET_LINK_LIBRARIES(${bin} scr_base ${SCR_EXTERNAL_SERIAL_LIBS})
    INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${bin} DESTINATION ${CMAKE_INSTALL_BINDIR})
ENDFOREACH(bin IN ITEMS ${cliscr_c_bins})

# Build and install CLI binaries that link full SCR library
FOREACH(bin IN ITEMS ${cliscr_scr_bins})
    ADD_EXECUTABLE(${bin} scr/src/${bin}.c)
    IF(SCR_LINK_STATIC)
      TARGET_LINK_LIBRARIES(${bin} scr-static)
    ELSE(SCR_LINK_STATIC)
      TARGET_LINK_LIBRARIES(${bin} scr)
    ENDIF(SCR_LINK_STATIC)
    INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${bin} DESTINATION ${CMAKE_INSTALL_BINDIR})
ENDFOREACH(bin IN ITEMS ${cliscr_scr_bins})

# some projects require a "make install" command to work,
# so define at least a basic INSTALL function
INSTALL(FILES NOTICE DESTINATION share/scr)

# Package
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Scalable Checkpoint / Restart Library")
SET(CPACK_PACKAGE_VENDOR "Lawrence Livermore National Laboratory")
SET(CPACK_PACKAGE_DESCRIPTION_FILE "${PROJECT_SOURCE_DIR}/scr/README.md")
SET(CPACK_RESOURCE_FILE_LICENSE "${PROJECT_SOURCE_DIR}/scr/LICENSE.TXT")
