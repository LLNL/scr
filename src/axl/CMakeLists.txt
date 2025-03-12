LIST(APPEND libaxl_srcs
    axl.c
    axl_sync.c
    axl_err.c
    axl_io.c
    axl_util.c
)

IF(ENABLE_PTHREADS)
  LIST(APPEND libaxl_srcs axl_pthread.c)
ENDIF(ENABLE_PTHREADS)

IF(BBAPI_FOUND)
    LIST(APPEND libaxl_srcs axl_async_bbapi.c)
ENDIF(BBAPI_FOUND)

IF(HAVE_DATAWARP)
    LIST(APPEND libaxl_srcs axl_async_datawarp.c)
ENDIF(HAVE_DATAWARP)

IF(MPI AND MPI_FOUND)
  LIST(APPEND libaxl_srcs axl_mpi.c)
ENDIF(MPI AND MPI_FOUND)

ADD_LIBRARY(axl_o OBJECT ${libaxl_srcs})
ADD_LIBRARY(axl_noMPI_o OBJECT ${libaxl_srcs})

IF(BUILD_SHARED_LIBS)
  SET_PROPERTY(TARGET axl_o PROPERTY POSITION_INDEPENDENT_CODE 1)
  SET_PROPERTY(TARGET axl_noMPI_o PROPERTY POSITION_INDEPENDENT_CODE 1)
ENDIF()
