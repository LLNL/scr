# Header files need not be enumerated
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_BINARY_DIR})

# Install header files
LIST(APPEND libredset_install_headers
    redset.h
)

LIST(APPEND redset_noMPI_srcs
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

ADD_LIBRARY(redset_o OBJECT ${libredset_srcs})
TARGET_COMPILE_DEFINITIONS(redset_o PRIVATE REDSET_ENABLE_MPI)

ADD_LIBRARY(redset_noMPI_o OBJECT ${redset_noMPI_srcs})
