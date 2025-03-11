# Header files need not be enumerated
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_BINARY_DIR})

LIST(APPEND libshuffile_srcs
    shuffile_util.c
    shuffile_io.c
    shuffile.c
)

ADD_LIBRARY(shuffile_o OBJECT $<TARGET_OBJECTS:redset_o> ${libshuffile_srcs})
