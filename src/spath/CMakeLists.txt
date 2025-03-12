IF(TVDISPLAY)
    ADD_DEFINITIONS(-DHAVE_TV)
ENDIF(TVDISPLAY)

LIST(APPEND libspath_serial_srcs
    spath.c
    spath_util.c
)
IF(TVDISPLAY)
    LIST(APPEND libspath_serial_srcs tv_data_display.c)
ENDIF(TVDISPLAY)

LIST(APPEND libspath_srcs
    spath.c
    spath_util.c
)

IF(MPI)
    LIST(APPEND libspath_srcs spath_mpi.c)
ENDIF(MPI)

IF(TVDISPLAY)
    LIST(APPEND libspath_srcs tv_data_display.c)
ENDIF(TVDISPLAY)

ADD_LIBRARY(spath_o OBJECT ${libspath_srcs})
ADD_LIBRARY(spath_serial_o OBJECT ${libspath_serial_srcs})

IF(BUILD_SHARED_LIBS)
  SET_PROPERTY(TARGET spath_o PROPERTY POSITION_INDEPENDENT_CODE 1)
  SET_PROPERTY(TARGET spath_serial_o PROPERTY POSITION_INDEPENDENT_CODE 1)
ENDIF(BUILD_SHARED_LIBS)
