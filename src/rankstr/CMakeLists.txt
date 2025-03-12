LIST(APPEND librankstr_srcs
    rankstr_mpi.c
)

ADD_LIBRARY(rankstr_o OBJECT ${librankstr_srcs})

IF(BUILD_SHARED_LIBS)
  SET_PROPERTY(TARGET rankstr_o PROPERTY POSITION_INDEPENDENT_CODE 1)
ENDIF()
