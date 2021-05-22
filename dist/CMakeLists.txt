PROJECT(SCR_TOP)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# force debug flags to be "-g -O0" instead of "-g"
SET(CMAKE_C_FLAGS_DEBUG   "-g -O0")
SET(CMAKE_CXX_FLAGS_DEBUG "-g -O0")

# Mac rpath Policy
IF(POLICY CMP0042)
    CMAKE_POLICY(SET CMP0042 NEW)
ENDIF(POLICY CMP0042)
SET(CMAKE_MACOSX_RPATH ON)

# Linux rpath settings
SET(CMAKE_SKIP_BUILD_RPATH FALSE)
SET(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

SET(CMAKE_POSITION_INDEPENDENT_CODE ON)

###################
# SCR CMake options
###################
OPTION(BUILD_SHARED_LIBS "Whether to build shared libraries" ON)
MESSAGE(STATUS "BUILD_SHARED_LIBS: ${BUILD_SHARED_LIBS}")

if(BUILD_SHARED_LIBS)
    OPTION(SCR_LINK_STATIC "Default to static linking? (Needed for Cray)" OFF)
else()
    OPTION(SCR_LINK_STATIC "Default to static linking? (Needed for Cray)" ON)
endif()
MESSAGE(STATUS "SCR_LINK_STATIC: ${SCR_LINK_STATIC}")

SET(SCR_RESOURCE_MANAGER "SLURM" CACHE STRING "Resource Manager for CLI (SLURM APRUN PMIX LSF NONE)")
SET_PROPERTY(CACHE SCR_RESOURCE_MANAGER PROPERTY STRINGS SLURM APRUN PMIX LSF NONE)
MESSAGE(STATUS "SCR_RESOURCE_MANAGER: ${SCR_RESOURCE_MANAGER}")

OPTION(ENABLE_IBM_BBAPI "Whether to enable IBM Burst Buffer support" ON)
MESSAGE(STATUS "ENABLE_IBM_BBAPI: ${ENABLE_IBM_BBAPI}")

OPTION(ENABLE_CRAY_DW "Whether to enable Cray Datawarp support" OFF)
MESSAGE(STATUS "ENABLE_CRAY_DW: ${ENABLE_CRAY_DW}")

OPTION(ENABLE_INTEL_CPPR "Whether to enable Intel CPPR support" OFF)
MESSAGE(STATUS "ENABLE_INTEL_CPPR: ${ENABLE_INTEL_CPPR}")

SET(SCR_CACHE_BASE "/dev/shm" CACHE PATH "Default base path for SCR cache directory")
SET(SCR_CNTL_BASE "/dev/shm" CACHE PATH "Default base path for SCR control directory")
SET(SCR_CONFIG_FILE "/etc/scr/scr.conf" CACHE FILEPATH "Default full path and filename for SCR config file")
MESSAGE(STATUS "SCR_CACHE_BASE: ${SCR_CACHE_BASE}")
MESSAGE(STATUS "SCR_CNTL_BASE: ${SCR_CNTL_BASE}")
MESSAGE(STATUS "SCR_CONFIG_FILE: ${SCR_CONFIG_FILE}")

SET(SCR_LOG_ENABLE "0" CACHE STRING "Whether to enable logging of any type (1) or not (0)")
SET_PROPERTY(CACHE SCR_LOG_ENABLE PROPERTY STRINGS 0 1)
SET(SCR_LOG_TXT_ENABLE "1" CACHE STRING "Whether to enable text file logging (1) or not (0)")
SET_PROPERTY(CACHE SCR_LOG_TXT_ENABLE PROPERTY STRINGS 0 1)
SET(SCR_LOG_SYSLOG_ENABLE "1" CACHE STRING "Whether to enable syslog logging (1) or not (0)")
SET_PROPERTY(CACHE SCR_LOG_SYSLOG_ENABLE PROPERTY STRINGS 0 1)
SET(SCR_LOG_SYSLOG_PREFIX "SCR" CACHE STRING "String to use as prefix on all syslog messages")
SET(SCR_LOG_SYSLOG_FACILITY "LOG_LOCAL7" CACHE STRING "Facility to use on syslog messages (see man openlog)")
SET(SCR_LOG_SYSLOG_LEVEL "LOG_INFO" CACHE STRING "Level to use on all syslog messages (see man openlog)")
MESSAGE(STATUS "SCR_LOG_ENABLE: ${SCR_LOG_ENABLE}")
MESSAGE(STATUS "SCR_LOG_TXT_ENABLE: ${SCR_LOG_TXT_ENABLE}")
MESSAGE(STATUS "SCR_LOG_SYSLOG_ENABLE: ${SCR_LOG_SYSLOG_ENABLE}")
MESSAGE(STATUS "SCR_LOG_SYSLOG_PREFIX: ${SCR_LOG_SYSLOG_PREFIX}")
MESSAGE(STATUS "SCR_LOG_SYSLOG_FACILITY: ${SCR_LOG_SYSLOG_FACILITY}")
MESSAGE(STATUS "SCR_LOG_SYSLOG_LEVEL: ${SCR_LOG_SYSLOG_LEVEL}")

SET(SCR_FILE_LOCK "FLOCK" CACHE STRING "Specify type of file locking to use (FLOCK FCNTL NONE)")
SET_PROPERTY(CACHE SCR_FILE_LOCK PROPERTY STRINGS FLOCK FCNTL NONE)
SET(KVTREE_FILE_LOCK ${SCR_FILE_LOCK})
MESSAGE(STATUS "SCR_FILE_LOCK: ${SCR_FILE_LOCK}")

OPTION(SCR_BGQ "Enable proper BGQ compilation" OFF)
MESSAGE(STATUS "SCR_BGQ: ${SCR_BGQ}")

OPTION(ENABLE_FORTRAN "Turn Fortran Support ON/OFF" ON)
MESSAGE(STATUS "ENABLE_FORTRAN: ${ENABLE_FORTRAN}")
IF(ENABLE_FORTRAN)
    SET(HAVE_FORTRAN_API TRUE)
    ENABLE_LANGUAGE(Fortran)
ENDIF(ENABLE_FORTRAN)

OPTION(ENABLE_YOGRT "Enable use of yogrt" ON)
MESSAGE(STATUS "ENABLE_YOGRT: ${ENABLE_YOGRT}")

OPTION(ENABLE_MYSQL "Enable use of mysql" ON)
MESSAGE(STATUS "ENABLE_MYSQL: ${ENABLE_MYSQL}")

OPTION(ENABLE_PDSH "Enable use of pdsh" ON)
MESSAGE(STATUS "ENABLE_PDSH: ${ENABLE_PDSH}")
IF(ENABLE_PDSH)
    OPTION(BUILD_PDSH "Download and build the PDSH Library?" OFF)
    MESSAGE(STATUS "BUILD_PDSH: ${BUILD_PDSH}")
ENDIF(ENABLE_PDSH)

OPTION(ENABLE_TESTS "Build tests" ON)
MESSAGE(STATUS "ENABLE_TESTS: ${ENABLE_TESTS}")

OPTION(ENABLE_EXAMPLES "Build examples" ON)
MESSAGE(STATUS "ENABLE_EXAMPLES: ${ENABLE_EXAMPLES}")

#LIST(APPEND CMAKE_OPTS "-DZLIB_ROOT=${ZLIB_ROOT}")

###################

# Find Packages & Files

LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/scr/cmake")
LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/axl/cmake")

## MPI
INCLUDE(SetupMPI)
IF(MPI_C_FOUND)
    INCLUDE_DIRECTORIES(${MPI_C_INCLUDE_PATH})
    LIST(APPEND SCR_EXTERNAL_LIBS ${MPI_C_LIBRARIES})
ELSE(MPI_C_FOUND)
    MESSAGE("WARNING: Could not find MPI!")
    MESSAGE("         Either add an MPI compiler to your path (using modules)")
    MESSAGE("         Or force CMake to build using the correct compiler (`export CC=mpicc`)")
ENDIF(MPI_C_FOUND)

## ZLIB
FIND_PACKAGE(ZLIB REQUIRED)
IF(ZLIB_FOUND)
    INCLUDE_DIRECTORIES(${ZLIB_INCLUDE_DIRS})
    LIST(APPEND SCR_EXTERNAL_LIBS ${ZLIB_LIBRARIES})
    LIST(APPEND SCR_EXTERNAL_SERIAL_LIBS ${ZLIB_LIBRARIES})
    LIST(APPEND SCR_LINK_LINE "-lz")
ENDIF(ZLIB_FOUND)

# PTHREADS
FIND_PACKAGE(Threads REQUIRED)
IF(CMAKE_USE_PTHREADS_INIT)
    LIST(APPEND SCR_EXTERNAL_LIBS "-lpthread")
ENDIF()

## PMIx
IF(${SCR_RESOURCE_MANAGER} STREQUAL "PMIX")
    FIND_PACKAGE(PMIX REQUIRED)
    SET(HAVE_PMIX TRUE)
    INCLUDE_DIRECTORIES(${PMIX_INCLUDE_DIRS})
    LIST(APPEND SCR_EXTERNAL_LIBS ${PMIX_LIBRARIES})
ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "PMIX")

## CPPR
IF(ENABLE_INTEL_CPPR)
    FIND_PACKAGE(CPPR REQUIRED)
    SET(HAVE_CPPR TRUE)
    INCLUDE_DIRECTORIES(${CPPR_INCLUDE_DIRS})
    LIST(APPEND SCR_EXTERNAL_LIBS ${CPPR_LIBRARIES})
ENDIF(ENABLE_INTEL_CPPR)

## DataWarp
IF(ENABLE_CRAY_DW)
    FIND_PACKAGE(DataWarp REQUIRED)
    SET(HAVE_DATAWARP TRUE)
    INCLUDE_DIRECTORIES(${DATAWARP_INCLUDE_DIRS})
    LIST(APPEND SCR_EXTERNAL_LIBS ${DATAWARP_LIBRARIES})
    # LIST(APPEND AXL_LINK_LINE " -L${WITH_DATAWARP_PREFIX}/lib64 -ldatawarp")
ENDIF(ENABLE_CRAY_DW)

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
        LIST(APPEND AXL_LINK_LINE " -L${WITH_BBAPI_PREFIX}/lib -lbbAPI")
    ENDIF(BBAPI_FOUND)
ENDIF(ENABLE_IBM_BBAPI)

## libyogrt
IF(ENABLE_YOGRT)
    FIND_PACKAGE(YOGRT QUIET)
    IF(YOGRT_FOUND)
        SET(HAVE_LIBYOGRT TRUE)
        INCLUDE_DIRECTORIES(${YOGRT_INCLUDE_DIRS})
        LIST(APPEND SCR_EXTERNAL_LIBS ${YOGRT_LIBRARIES})
        LIST(APPEND SCR_LINK_LINE " -L${WITH_YOGRT_PREFIX}/lib -lyogrt")
    ENDIF(YOGRT_FOUND)
ENDIF(ENABLE_YOGRT)

## mySQL
IF(ENABLE_MYSQL)
    FIND_PACKAGE(MySQL)
    IF(MYSQL_FOUND)
        SET(HAVE_LIBMYSQLCLIENT TRUE)
        INCLUDE_DIRECTORIES(${MYSQL_INCLUDE_DIRS})
        LIST(APPEND SCR_EXTERNAL_LIBS ${MYSQL_LIBRARIES})
        LIST(APPEND SCR_EXTERNAL_SERIAL_LIBS ${MYSQL_LIBRARIES})
        LIST(APPEND SCR_LINK_LINE " -L${WITH_MYSQL_PREFIX}/lib -lmysqlclient")
    ENDIF(MYSQL_FOUND)
ENDIF(ENABLE_MYSQL)

## PDSH
IF(ENABLE_PDSH)
    IF(BUILD_PDSH)
        INCLUDE(ExternalProject)
        EXTERNALPROJECT_ADD(PDSH
            GIT_REPOSITORY    https://github.com/grondo/pdsh
            PREFIX            ${CMAKE_CURRENT_BINARY_DIR}/PDSH
            UPDATE_COMMAND    ${CMAKE_CURRENT_BINARY_DIR}/PDSH/src/PDSH/bootstrap
            CONFIGURE_COMMAND ${CMAKE_CURRENT_BINARY_DIR}/PDSH/src/PDSH/configure --prefix=${CMAKE_INSTALL_PREFIX}
            BUILD_COMMAND     ${MAKE}
        )
        SET(PDSH_EXE ${CMAKE_INSTALL_PREFIX}/bin/pdsh)
        SET(DSHBAK_EXE ${CMAKE_INSTALL_PREFIX}/bin/dshbak)
    ELSE(BUILD_PDSH)
        FIND_PACKAGE(PDSH REQUIRED)
    ENDIF(BUILD_PDSH)
ENDIF(ENABLE_PDSH)

## HEADERS
INCLUDE(CheckIncludeFile)
CHECK_INCLUDE_FILE(byteswap.h HAVE_BYTESWAP_H)

INCLUDE(GNUInstallDirs)
## Use X_ variable names for CLI scripts
## could use CMAKE_INSTALL_FULL_ names instead
SET(X_BINDIR ${CMAKE_INSTALL_FULL_BINDIR} CACHE INTERNAL "bin")
SET(X_DATADIR ${CMAKE_INSTALL_FULL_DATADIR} CACHE INTERNAL "share")
SET(X_INCLUDEDIR ${CMAKE_INSTALL_FULL_INCLUDEDIR} CACHE INTERNAL "include")
SET(X_LIBDIR ${CMAKE_INSTALL_FULL_LIBDIR} CACHE INTERNAL "lib")

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
ADD_SUBDIRECTORY(scr/doc)
ADD_SUBDIRECTORY(scr/python)
IF(ENABLE_EXAMPLES)
    ADD_SUBDIRECTORY(scr/examples)
ENDIF()

# Generate config.h with all our build #defs
CONFIGURE_FILE(${PROJECT_SOURCE_DIR}/cmake/config.h.in ${PROJECT_BINARY_DIR}/config.h)
SET_SOURCE_FILES_PROPERTIES(config.h.in PROPERTIES GENERATED FALSE)
SET_SOURCE_FILES_PROPERTIES(${PROJECT_BINARY_DIR}/config.h PROPERTIES GENERATED TRUE)

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

LIST(APPEND libkvtree_noMPI_srcs
    kvtree/src/kvtree.c
    kvtree/src/kvtree_util.c
    kvtree/src/kvtree_io.c
    kvtree/src/kvtree_helpers.c
    kvtree/src/kvtree_err.c
)
LIST(APPEND libkvtree_srcs
    kvtree/src/kvtree.c
    kvtree/src/kvtree_util.c
    kvtree/src/kvtree_io.c
    kvtree/src/kvtree_helpers.c
    kvtree/src/kvtree_err.c
)
LIST(APPEND libkvtree_srcs
    kvtree/src/kvtree_mpi.c
    kvtree/src/kvtree_mpi_io.c
)

#IF(TVDISPLAY)
#    LIST(APPEND libkvtree_srcs
#        kvtree/src/tv_data_display.c
#    )
#ENDIF(TVDISPLAY)

LIST(APPEND libspath_noMPI_srcs
    spath/src/spath.c
    spath/src/spath_util.c
)
LIST(APPEND libspath_srcs
    spath/src/spath.c
    spath/src/spath_util.c
)
LIST(APPEND libspath_srcs spath/src/spath_mpi.c)

LIST(APPEND librankstr_srcs
    rankstr/src/rankstr_mpi.c
)

LIST(APPEND libaxl_srcs
    axl/src/axl.c
    axl/src/axl_sync.c
    axl/src/axl_async_datawarp.c
    axl/src/axl_async_bbapi.c
#    axl/src/axl_async_cppr.c
    axl/src/axl_err.c
    axl/src/axl_io.c
    axl/src/axl_util.c
    axl/src/axl_pthread.c
)

LIST(APPEND libshuffile_srcs
    shuffile/src/shuffile_util.c
    shuffile/src/shuffile_io.c
    shuffile/src/shuffile.c
)

LIST(APPEND redset_noMPI_srcs
    redset/src/redset_err_serial.c
    redset/src/redset_util.c
    redset/src/redset_io.c
    redset/src/redset_lofi.c
    redset/src/redset_partner_serial.c
    redset/src/redset_xor_serial.c
    redset/src/redset_reedsolomon_common.c
    redset/src/redset_reedsolomon_serial.c
)

LIST(APPEND libredset_srcs
    redset/src/redset_err_mpi.c
    redset/src/redset_util.c
    redset/src/redset_util_mpi.c
    redset/src/redset_io.c
    redset/src/redset_lofi.c
    redset/src/redset.c
    redset/src/redset_single.c
    redset/src/redset_partner.c
    redset/src/redset_xor.c
    redset/src/redset_reedsolomon_common.c
    redset/src/redset_reedsolomon.c
)

LIST(APPEND liber_srcs
    er/src/er.c
    er/src/er_util.c
)

LIST(APPEND cliscr_noMPI_srcs
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
)

LIST(APPEND libscr_srcs
    scr/src/scr.c
    scr/src/scr_cache.c
    scr/src/scr_cache_rebuild.c
    scr/src/scr_cache_index.c
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
)

ADD_LIBRARY(scr_o OBJECT
    ${lwgrp_srcs}
    ${dtcmp_srcs}
    ${libkvtree_srcs}
    ${libspath_srcs}
    ${librankstr_srcs}
    ${libaxl_srcs}
    ${libshuffile_srcs}
    ${libredset_srcs}
    ${liber_srcs}
    ${libscr_srcs}
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
    ADD_LIBRARY(scr SHARED $<TARGET_OBJECTS:scr_o>)
    TARGET_LINK_LIBRARIES(scr ${SCR_EXTERNAL_LIBS})
    SET_TARGET_PROPERTIES(scr PROPERTIES OUTPUT_NAME scr CLEAN_DIRECT_OUTPUT 1)
    INSTALL(TARGETS scr DESTINATION ${CMAKE_INSTALL_LIBDIR})
ENDIF()

ADD_LIBRARY(scr-static STATIC $<TARGET_OBJECTS:scr_o>)
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
        ADD_LIBRARY(scrf SHARED scr/src/scrf.c $<TARGET_OBJECTS:scr_o>)
        TARGET_LINK_LIBRARIES(scrf ${SCR_EXTERNAL_LIBS})
        SET_TARGET_PROPERTIES(scrf PROPERTIES OUTPUT_NAME scrf CLEAN_DIRECT_OUTPUT 1)
        INSTALL(TARGETS scrf DESTINATION ${CMAKE_INSTALL_LIBDIR})
    ENDIF()

    ADD_LIBRARY(scrf-static STATIC scr/src/scrf.c $<TARGET_OBJECTS:scr_o>)
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
    ${libkvtree_noMPI_srcs}
    ${libspath_noMPI_srcs}
    ${redset_noMPI_srcs}
    ${cliscr_noMPI_srcs}
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
    TARGET_LINK_LIBRARIES(${bin} scr_base)
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
