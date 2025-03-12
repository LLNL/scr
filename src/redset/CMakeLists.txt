LIST(APPEND redset_serial_srcs
    redset_err_serial.c
    redset_util.c
    redset_io.c
    redset_lofi.c
    redset_partner_serial.c
    redset_xor_serial.c
    redset_reedsolomon_common.c
    redset_reedsolomon_serial.c
)

LIST(APPEND libredset_srcs
    redset_err_mpi.c
    redset_util.c
    redset_util_mpi.c
    redset_io.c
    redset_lofi.c
    redset.c
    redset_single.c
    redset_partner.c
    redset_xor.c
    redset_reedsolomon.c
    redset_reedsolomon_common.c
)

IF(ENABLE_PTHREADS)
    LIST(APPEND libredset_srcs
        redset_xor_pthreads.c
        redset_reedsolomon_pthreads.c
    )
ENDIF(ENABLE_PTHREADS)

IF(ENABLE_CUDA)
    LIST(APPEND libredset_srcs
        redset_xor_gpu.cu
        redset_reedsolomon_gpu.cu
    )
ENDIF(ENABLE_CUDA)

ADD_LIBRARY(redset_o OBJECT $<TARGET_OBJECTS:kvtree_o> $<TARGET_OBJECTS:rankstr_o> ${libredset_srcs})
ADD_LIBRARY(redset_serial_o OBJECT $<TARGET_OBJECTS:kvtree_serial_o> $<TARGET_OBJECTS:rankstr_o> ${redset_serial_srcs})

IF(BUILD_SHARED_LIBS)
  SET_PROPERTY(TARGET redset_o PROPERTY POSITION_INDEPENDENT_CODE 1)
  SET_PROPERTY(TARGET redset_serial_o PROPERTY POSITION_INDEPENDENT_CODE 1)
ENDIF()

TARGET_COMPILE_DEFINITIONS(redset_o PRIVATE REDSET_ENABLE_MPI)
