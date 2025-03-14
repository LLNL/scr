IF(TVDISPLAY)
    ADD_DEFINITIONS(-DHAVE_TV)
ENDIF(TVDISPLAY)

LIST(APPEND libkvtree_serial_srcs
    kvtree.c
    kvtree_util.c
    kvtree_io.c
    kvtree_helpers.c
    kvtree_err.c
)

IF(TVDISPLAY)
    LIST(APPEND libkvtree_serial_srcs tv_data_display.c)
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

ADD_LIBRARY(kvtree_o OBJECT ${libkvtree_srcs})
ADD_LIBRARY(kvtree_serial_o OBJECT ${libkvtree_serial_srcs})

IF(BUILD_SHARED_LIBS)
  SET_PROPERTY(TARGET kvtree_o PROPERTY POSITION_INDEPENDENT_CODE 1)
  SET_PROPERTY(TARGET kvtree_serial_o PROPERTY POSITION_INDEPENDENT_CODE 1)
ENDIF()

TARGET_LINK_LIBRARIES(kvtree_o PUBLIC ${SCR_EXTERNAL_LIBS})
TARGET_LINK_LIBRARIES(kvtree_serial_o PUBLIC ${SCR_EXTERNAL_SERIAL_LIBS})

##############
# CLI KVTREE #
##############

# Individual binaries generated from a single .c file
LIST(APPEND serial_kvtree_c_bins
    kvtree_print
)

# Build and install C binaries
FOREACH(bin IN ITEMS ${serial_kvtree_c_bins})
    ADD_EXECUTABLE(${bin} ${bin}.c)
    TARGET_LINK_LIBRARIES(${bin} kvtree_serial_o)
ENDFOREACH(bin IN ITEMS ${serial_kvtree_c_bins})
