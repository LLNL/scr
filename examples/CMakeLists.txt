# Install examples source code

LIST(APPEND example_files
	test_common.c
	test_common.h
	test_api.c
	test_api_file.c
	test_api_multiple.c
	test_api_multiple_file.c
	test_interpose.c
	test_interpose_multiple.c
	test_ckpt.C
	test_ckpt.F
	scr.moab
	scr_interpose.moab
	README.md
)
INSTALL(FILES ${example_files} DESTINATION ${CMAKE_INSTALL_DATADIR}/scr/examples)

## static linking means we must pass a link line to our examples
## TODO: maybe this can be done a better way?
IF(NOT "${SCR_LINK_LINE}" STREQUAL "")
        STRING(REPLACE ";" " " SCR_LINK_LINE ${SCR_LINK_LINE})
ENDIF(NOT "${SCR_LINK_LINE}" STREQUAL "")

## Configure & install makefile
CONFIGURE_FILE(makefile.examples.in makefile.examples @ONLY)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/makefile.examples.in PROPERTIES GENERATED FALSE)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/makefile.examples PROPERTIES GENERATED TRUE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/makefile.examples DESTINATION ${CMAKE_INSTALL_DATADIR}/scr/examples RENAME makefile)

# CMAKE PROPER FOR EXAMPLES

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR})

## Move test cleanup script to build directory
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_cleanup.sh DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

ADD_EXECUTABLE(test_api test_common.c test_api.c)
TARGET_LINK_LIBRARIES(test_api scr)
SCR_ADD_TEST(test_api "" "ckpt.*")

ADD_EXECUTABLE(test_api_file test_common.c test_api_file.c)
TARGET_LINK_LIBRARIES(test_api_file scr)
#SCR_ADD_TEST: proper usage is unknown

ADD_EXECUTABLE(test_api_multiple test_common.c test_api_multiple.c)
TARGET_LINK_LIBRARIES(test_api_multiple scr)
SCR_ADD_TEST(test_api_multiple "" "")

ADD_EXECUTABLE(test_api_multiple_file test_common.c test_api_multiple_file.c)
TARGET_LINK_LIBRARIES(test_api_multiple_file scr)
#SCR_ADD_TEST: proper usage is unknown

ADD_EXECUTABLE(test_interpose test_interpose.c)
TARGET_LINK_LIBRARIES(test_interpose scr)
SCR_ADD_TEST(test_interpose "" "checkpoint_set_*")

ADD_EXECUTABLE(test_interpose_multiple test_common.c test_interpose_multiple.c)
TARGET_LINK_LIBRARIES(test_interpose_multiple scr)
SCR_ADD_TEST(test_interpose_multiple "" "examplesrank_0.0.ckpt")

ADD_EXECUTABLE(test_ckpt_C test_ckpt.C)
TARGET_LINK_LIBRARIES(test_ckpt_C scr)
SCR_ADD_TEST(test_ckpt_C "" "rank_0.ckpt")

IF(ENABLE_FORTRAN)
	SET_SOURCE_FILES_PROPERTIES(test_ckpt.F PROPERTIES LANGUAGE Fortran)
	ADD_EXECUTABLE(test_ckpt_F test_ckpt.F)
	TARGET_LINK_LIBRARIES(test_ckpt_F scrf ${MPI_Fortran_LIBRARIES})
	SCR_ADD_TEST(test_ckpt_F "" "rank_00000.ckpt")
ENDIF(ENABLE_FORTRAN)

# add target: "make check"
# prints output from the ctests
ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
