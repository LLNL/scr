# Header files need not be enumerated
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_BINARY_DIR})

LIST(APPEND liber_srcs
    er.c
    er_util.c
)

ADD_LIBRARY(er_o OBJECT $<TARGET_OBJECTS:redset_o> ${liber_srcs})
TARGET_COMPILE_DEFINITIONS(er_o PRIVATE REDSET_ENABLE_MPI)
