LIST(APPEND liber_srcs
    er.c
    er_util.c
)

ADD_LIBRARY(er_o OBJECT $<TARGET_OBJECTS:redset_o> ${liber_srcs})
