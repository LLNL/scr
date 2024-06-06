# Header files need not be enumerated
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_BINARY_DIR})

###########
# LIB SCR #
###########

# Install header files
LIST(APPEND libscr_install_headers
    scr.h
)
IF(ENABLE_FORTRAN)
    LIST(APPEND libscr_install_headers scrf.h)
ENDIF(ENABLE_FORTRAN)
INSTALL(FILES ${libscr_install_headers} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

## CLI should build without MPI dependence
LIST(APPEND cliscr_noMPI_srcs
    scr_cache_index.c
    scr_cache_index_serial.c
    scr_config.c
    scr_config_serial.c
    scr_dataset.c
    scr_env.c
    scr_err_serial.c
    scr_filemap.c
    scr_flush_nompi.c
    scr_halt.c
    scr_index_api.c
    scr_io.c
    scr_log.c
    scr_meta.c
    scr_param.c
    scr_util.c
    scr_rebuild_xor.c
    scr_rebuild_partner.c
    scr_rebuild_rs.c
)

LIST(APPEND libscr_srcs
    scr.c
    scr_cache.c
    scr_cache_rebuild.c
    scr_cache_index.c
    scr_cache_index_mpi.c
    scr_config.c
    scr_config_mpi.c
    scr_dataset.c
    scr_dataset.c
    scr_env.c
    scr_err_mpi.c
    scr_fetch.c
    scr_filemap.c
    scr_flush.c
    scr_flush_file_mpi.c
    scr_flush_nompi.c
    scr_flush_sync.c
    scr_flush_async.c
    scr_globals.c
    scr_groupdesc.c
    scr_halt.c
    scr_index_api.c
    scr_io.c
    scr_log.c
    scr_meta.c
    scr_param.c
    scr_prefix.c
    scr_reddesc.c
    scr_storedesc.c
    scr_summary.c
    scr_util.c
    scr_util_mpi.c
)

IF(BUILD_SHARED_LIBS)
   # SCR Library
   ADD_LIBRARY(scr_o OBJECT ${libscr_srcs})
   TARGET_LINK_LIBRARIES(scr_o PRIVATE ${SCR_EXTERNAL_LIBS})

   ADD_LIBRARY(scr SHARED $<TARGET_OBJECTS:scr_o>)
   ADD_LIBRARY(scr::scr ALIAS scr)
   TARGET_LINK_LIBRARIES(scr PUBLIC ${SCR_EXTERNAL_LIBS})
   TARGET_INCLUDE_DIRECTORIES(scr PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
   SET_TARGET_PROPERTIES(scr PROPERTIES OUTPUT_NAME scr CLEAN_DIRECT_OUTPUT 1)
   INSTALL(TARGETS scr EXPORT scrTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
ENDIF()

# SCR Library
ADD_LIBRARY(scr_static_o OBJECT ${libscr_srcs})
TARGET_LINK_LIBRARIES(scr_static_o PRIVATE ${SCR_EXTERNAL_STATIC_LIBS})

ADD_LIBRARY(scr-static STATIC $<TARGET_OBJECTS:scr_static_o>)
ADD_LIBRARY(scr::scr-static ALIAS scr-static)
IF(SCR_LINK_STATIC)
    SET_TARGET_PROPERTIES(scr-static PROPERTIES LINK_SEARCH_START_STATIC 1)
    SET_TARGET_PROPERTIES(scr-static PROPERTIES LINK_SEARCH_END_STATIC 1)
ENDIF(SCR_LINK_STATIC)
TARGET_LINK_LIBRARIES(scr-static PUBLIC ${SCR_EXTERNAL_STATIC_LIBS})
TARGET_INCLUDE_DIRECTORIES(scr-static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
SET_TARGET_PROPERTIES(scr-static PROPERTIES OUTPUT_NAME scr CLEAN_DIRECT_OUTPUT 1)
INSTALL(TARGETS scr-static EXPORT scrTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# Non-MPI library for CLI
ADD_LIBRARY(scr_base_o OBJECT ${cliscr_noMPI_srcs})
TARGET_LINK_LIBRARIES(scr_base_o PRIVATE ${SCR_EXTERNAL_SERIAL_STATIC_LIBS})

ADD_LIBRARY(scr_base STATIC $<TARGET_OBJECTS:scr_base_o>)
ADD_LIBRARY(scr::scr_base ALIAS scr_base)
TARGET_LINK_LIBRARIES(scr_base PUBLIC ${SCR_EXTERNAL_SERIAL_STATIC_LIBS})

# Fortran
IF(ENABLE_FORTRAN)
        IF(BUILD_SHARED_LIBS)
           ADD_LIBRARY(scrf SHARED scrf.c $<TARGET_OBJECTS:scr_o>)
           ADD_LIBRARY(scr::scrf ALIAS scrf)
           TARGET_LINK_LIBRARIES(scrf PUBLIC ${SCR_EXTERNAL_LIBS})
           TARGET_INCLUDE_DIRECTORIES(scrf PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
           SET_TARGET_PROPERTIES(scrf PROPERTIES OUTPUT_NAME scrf CLEAN_DIRECT_OUTPUT 1)
           INSTALL(TARGETS scrf EXPORT scrTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
        ENDIF()

        ADD_LIBRARY(scrf-static STATIC scrf.c $<TARGET_OBJECTS:scr_static_o>)
        ADD_LIBRARY(scr::scrf-static ALIAS scrf-static)
        IF(SCR_LINK_STATIC)
            SET_TARGET_PROPERTIES(scrf-static PROPERTIES LINK_SEARCH_START_STATIC 1)
            SET_TARGET_PROPERTIES(scrf-static PROPERTIES LINK_SEARCH_END_STATIC 1)
        ENDIF(SCR_LINK_STATIC)
        TARGET_LINK_LIBRARIES(scrf-static PUBLIC ${SCR_EXTERNAL_STATIC_LIBS})
        TARGET_INCLUDE_DIRECTORIES(scrf-static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
        SET_TARGET_PROPERTIES(scrf-static PROPERTIES OUTPUT_NAME scrf CLEAN_DIRECT_OUTPUT 1)
        INSTALL(TARGETS scrf-static EXPORT scrTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
ENDIF(ENABLE_FORTRAN)

###########
# CLI SCR #
###########

# Individual binaries generated from a single .c file
#    scr_inspect_cache
LIST(APPEND cliscr_c_bins
    scr_index
    scr_print
)

LIST(APPEND cliscr_c_libexec
    scr_crc32
    scr_flush_file
    scr_halt_cntl
    scr_log_event
    scr_log_transfer
    scr_nodes_file
    scr_retries_halt
    scr_copy
    scr_rebuild_xor
    scr_rebuild_partner
    scr_rebuild_rs
)

# CLI binaries that require full SCR library
#LIST(APPEND cliscr_scr_bins
#    scr_have_restart
#)

# Build and install C binaries
FOREACH(bin IN ITEMS ${cliscr_c_bins})
    ADD_EXECUTABLE(${bin} ${bin}.c)
    TARGET_LINK_LIBRARIES(${bin} scr_base)
    INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${bin} DESTINATION ${CMAKE_INSTALL_BINDIR})
ENDFOREACH(bin IN ITEMS ${cliscr_c_bins})

FOREACH(bin IN ITEMS ${cliscr_c_libexec})
    ADD_EXECUTABLE(${bin} ${bin}.c)
    TARGET_LINK_LIBRARIES(${bin} scr_base)
    INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${bin} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR})
ENDFOREACH(bin IN ITEMS ${cliscr_c_libexec})

# Build and install CLI binaries that link full SCR library
FOREACH(bin IN ITEMS ${cliscr_scr_bins})
    ADD_EXECUTABLE(${bin} ${bin}.c)
    IF(SCR_LINK_STATIC)
        TARGET_LINK_LIBRARIES(${bin} scr-static)
    ELSE(SCR_LINK_STATIC)
        TARGET_LINK_LIBRARIES(${bin} scr)
    ENDIF(SCR_LINK_STATIC)
    INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${bin} DESTINATION ${CMAKE_INSTALL_BINDIR})
ENDFOREACH(bin IN ITEMS ${cliscr_scr_bins})
