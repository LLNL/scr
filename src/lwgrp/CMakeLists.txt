LIST(APPEND lwgrp_srcs
    lwgrp.c
    lwgrp_util.c
    lwgrp_chain_ops.c
    lwgrp_ring_ops.c
    lwgrp_logchain_ops.c
    lwgrp_logring_ops.c
    lwgrp_comm.c
    lwgrp_comm_split.c
)

ADD_LIBRARY(lwgrp_o OBJECT ${lwgrp_srcs})

IF(BUILD_SHARED_LIBS)
  SET_PROPERTY(TARGET lwgrp_o PROPERTY POSITION_INDEPENDENT_CODE 1)
ENDIF()
