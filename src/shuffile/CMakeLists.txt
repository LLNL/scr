LIST(APPEND libshuffile_srcs
    shuffile_util.c
    shuffile_io.c
    shuffile.c
)

ADD_LIBRARY(shuffile_o OBJECT $<TARGET_OBJECTS:kvtree_o> $<TARGET_OBJECTS:redset_o> ${libshuffile_srcs})

IF(BUILD_SHARED_LIBS)
  SET_PROPERTY(TARGET shuffile_o PROPERTY POSITION_INDEPENDENT_CODE 1)
ENDIF(BUILD_SHARED_LIBS)
