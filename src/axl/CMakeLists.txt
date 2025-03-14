IF(ENABLE_PTHREADS)
  LIST(APPEND axl_optional_srcs axl_pthread.c)
ENDIF(ENABLE_PTHREADS)

IF(BBAPI_FOUND)
    LIST(APPEND axl_optional_srcs axl_async_bbapi.c)
ENDIF(BBAPI_FOUND)

IF(HAVE_DATAWARP)
    LIST(APPEND axl_optional_srcs axl_async_datawarp.c)
ENDIF(HAVE_DATAWARP)

LIST(APPEND libaxl_srcs
    axl.c
    axl_sync.c
    axl_err.c
    axl_io.c
    axl_util.c
    ${axl_optional_srcs}
)

LIST(APPEND libaxl_serial_srcs
    axl.c
    axl_sync.c
    axl_err.c
    axl_io.c
    axl_util.c
    ${axl_optional_srcs}
)

IF(MPI_C_FOUND)
  LIST(APPEND libaxl_srcs axl_mpi.c)
ENDIF(MPI_C_FOUND)

ADD_LIBRARY(axl_o OBJECT $<TARGET_OBJECTS:kvtree_o> ${libaxl_srcs})
ADD_LIBRARY(axl_serial_o OBJECT $<TARGET_OBJECTS:kvtree_serial_o> ${libaxl_serial_srcs})

IF(BUILD_SHARED_LIBS)
  SET_PROPERTY(TARGET axl_o PROPERTY POSITION_INDEPENDENT_CODE 1)
  SET_PROPERTY(TARGET axl_serial_o PROPERTY POSITION_INDEPENDENT_CODE 1)
ENDIF()

IF(ENABLE_TESTS)
  ADD_SUBDIRECTORY(test)
ENDIF(ENABLE_TESTS)
