# Header files need not be enumerated
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_BINARY_DIR})

###########
# LIB KVTREE #
###########

IF(TVDISPLAY)
    ADD_DEFINITIONS(-DHAVE_TV)
ENDIF(TVDISPLAY)

# Install header files
LIST(APPEND libkvtree_install_headers
    kvtree.h
    kvtree_util.h
)
IF(MPI_FOUND)
    LIST(APPEND libkvtree_install_headers kvtree_mpi.h)
ENDIF(MPI_FOUND)
INSTALL(FILES ${libkvtree_install_headers} DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

LIST(APPEND libkvtree_noMPI_srcs
    kvtree.c
    kvtree_util.c
    kvtree_io.c
    kvtree_helpers.c
    kvtree_err.c
)

IF(TVDISPLAY)
    LIST(APPEND libkvtree_noMPI_srcs tv_data_display.c)
ENDIF(TVDISPLAY)

LIST(APPEND libkvtree_srcs
    kvtree.c
    kvtree_util.c
    kvtree_io.c
    kvtree_helpers.c
    kvtree_err.c
)

IF(TVDISPLAY)
    LIST(APPEND libkvtree_srcs tv_data_display.c)
ENDIF(TVDISPLAY)

IF(MPI_FOUND)
    LIST(APPEND libkvtree_srcs kvtree_mpi.c kvtree_mpi_io.c)
ENDIF(MPI_FOUND)

# KVTREE Library
ADD_LIBRARY(kvtree_o OBJECT ${libkvtree_srcs})
TARGET_LINK_LIBRARIES(kvtree_o PRIVATE ZLIB::ZLIB)
IF(MPI)
  TARGET_LINK_LIBRARIES(kvtree_o PRIVATE MPI::MPI_C)
ENDIF()
TARGET_INCLUDE_DIRECTORIES(kvtree_o PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

IF(BUILD_SHARED_LIBS)
   ADD_LIBRARY(kvtree SHARED $<TARGET_OBJECTS:kvtree_o>)
   ADD_LIBRARY(kvtree::kvtree ALIAS kvtree)
   TARGET_LINK_LIBRARIES(kvtree PUBLIC ZLIB::ZLIB)
   IF(MPI)
     TARGET_LINK_LIBRARIES(kvtree PUBLIC MPI::MPI_C)
   ENDIF()
   TARGET_INCLUDE_DIRECTORIES(kvtree PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
   SET_TARGET_PROPERTIES(kvtree PROPERTIES OUTPUT_NAME kvtree CLEAN_DIRECT_OUTPUT 1)
   INSTALL(TARGETS kvtree EXPORT kvtreeTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
ENDIF()

ADD_LIBRARY(kvtree-static STATIC $<TARGET_OBJECTS:kvtree_o>)
ADD_LIBRARY(kvtree::kvtree-static ALIAS kvtree-static)
TARGET_LINK_LIBRARIES(kvtree-static PUBLIC ZLIB::ZLIB)
IF(MPI)
  TARGET_LINK_LIBRARIES(kvtree-static PUBLIC MPI::MPI_C)
ENDIF()

TARGET_INCLUDE_DIRECTORIES(kvtree-static PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
IF(KVTREE_LINK_STATIC)
    SET_TARGET_PROPERTIES(kvtree-static PROPERTIES LINK_SEARCH_START_STATIC 1)
    SET_TARGET_PROPERTIES(kvtree-static PROPERTIES LINK_SEARCH_END_STATIC 1)
ENDIF(KVTREE_LINK_STATIC)

SET_TARGET_PROPERTIES(kvtree-static PROPERTIES OUTPUT_NAME kvtree CLEAN_DIRECT_OUTPUT 1)
INSTALL(TARGETS kvtree-static EXPORT kvtreeTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# KVTREE base Library (no MPI)
ADD_LIBRARY(kvtree_noMPI_o OBJECT ${libkvtree_noMPI_srcs})
TARGET_LINK_LIBRARIES(kvtree_noMPI_o PUBLIC ZLIB::ZLIB)

IF(BUILD_SHARED_LIBS)
   ADD_LIBRARY(kvtree_base SHARED $<TARGET_OBJECTS:kvtree_noMPI_o>)
   ADD_LIBRARY(kvtree::kvtree_base ALIAS kvtree_base)
   TARGET_INCLUDE_DIRECTORIES(kvtree_base PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
   TARGET_LINK_LIBRARIES(kvtree_base PUBLIC ZLIB::ZLIB)
   SET_TARGET_PROPERTIES(kvtree_base PROPERTIES OUTPUT_NAME kvtree_base CLEAN_DIRECT_OUTPUT 1)
   INSTALL(TARGETS kvtree_base EXPORT kvtreeTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
ENDIF(BUILD_SHARED_LIBS)

ADD_LIBRARY(kvtree_base-static STATIC $<TARGET_OBJECTS:kvtree_noMPI_o>)
ADD_LIBRARY(kvtree::kvtree_base-static ALIAS kvtree_base-static)
TARGET_INCLUDE_DIRECTORIES(kvtree_base-static PUBLIC  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}> $<INSTALL_INTERFACE:include>)
TARGET_LINK_LIBRARIES(kvtree_base-static PUBLIC ZLIB::ZLIB)
IF(KVTREE_LINK_STATIC)
    SET_TARGET_PROPERTIES(kvtree_base-static PROPERTIES LINK_SEARCH_START_STATIC 1)
    SET_TARGET_PROPERTIES(kvtree_base-static PROPERTIES LINK_SEARCH_END_STATIC 1)
ENDIF(KVTREE_LINK_STATIC)

SET_TARGET_PROPERTIES(kvtree_base-static PROPERTIES OUTPUT_NAME kvtree_base CLEAN_DIRECT_OUTPUT 1)
INSTALL(TARGETS kvtree_base-static EXPORT kvtreeTargets LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

###########
# CLI KVTREE #
###########

# Individual binaries generated from a single .c file
LIST(APPEND clikvtree_c_bins
    kvtree_print
)

# Build and install C binaries
FOREACH(bin IN ITEMS ${clikvtree_c_bins})
    ADD_EXECUTABLE(${bin} ${bin}.c)
    IF(KVTREE_LINK_STATIC)
        TARGET_LINK_LIBRARIES(${bin} kvtree-static)
    ELSE()
        TARGET_LINK_LIBRARIES(${bin} kvtree)
    ENDIF()
    INSTALL(PROGRAMS ${CMAKE_CURRENT_BINARY_DIR}/${bin} DESTINATION ${CMAKE_INSTALL_BINDIR})
ENDFOREACH(bin IN ITEMS ${clikvtree_c_bins})
