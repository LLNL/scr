LIST(APPEND libshuffile_srcs
    shuffile_util.c
    shuffile_io.c
    shuffile.c
)

ADD_LIBRARY(shuffile_o OBJECT $<TARGET_OBJECTS:redset_o> ${libshuffile_srcs})
