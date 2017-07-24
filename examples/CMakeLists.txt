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

ADD_EXECUTABLE(test_api test_common.c test_api.c)
TARGET_LINK_LIBRARIES(test_api scr)
ADD_TEST(NAME ctest_api COMMAND ./test_api)
SET_TESTS_PROPERTIES(ctest_api PROPERTIES ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/config.txt")

ADD_EXECUTABLE(test_api_file test_common.c test_api_file.c)
TARGET_LINK_LIBRARIES(test_api_file scr)
ADD_TEST(NAME ctest_api_file COMMAND ./test_api_file)
SET_TESTS_PROPERTIES(ctest_api_file PROPERTIES ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/config.txt")

ADD_EXECUTABLE(test_api_multiple test_common.c test_api_multiple.c)
TARGET_LINK_LIBRARIES(test_api_multiple scr)
ADD_TEST(NAME ctest_api_multiple COMMAND ./test_api_multiple)
SET_TESTS_PROPERTIES(ctest_api_multiple PROPERTIES ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/config.txt")

ADD_EXECUTABLE(test_api_multiple_file test_common.c test_api_multiple_file.c)
TARGET_LINK_LIBRARIES(test_api_multiple_file scr)
ADD_TEST(NAME ctest_api_multiple_file COMMAND ./test_api_multiple_file)
SET_TESTS_PROPERTIES(ctest_api_multiple_file PROPERTIES ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/config.txt")

ADD_EXECUTABLE(test_interpose test_interpose.c)
TARGET_LINK_LIBRARIES(test_interpose scr)
ADD_TEST(NAME ctest_interpose COMMAND ./test_interpose)
SET_TESTS_PROPERTIES(ctest_interpose PROPERTIES ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/config.txt")

ADD_EXECUTABLE(test_interpose_multiple test_common.c test_interpose_multiple.c)
TARGET_LINK_LIBRARIES(test_interpose_multiple scr)
ADD_TEST(NAME ctest_interpose_multiple COMMAND ./test_interpose_multiple)
SET_TESTS_PROPERTIES(ctest_interpose_multiple PROPERTIES ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/config.txt")

ADD_EXECUTABLE(test_ckpt_C test_ckpt.C)
TARGET_LINK_LIBRARIES(test_ckpt_C scr)
ADD_TEST(NAME ctest_ckpt_C COMMAND ./test_ckpt_C)
SET_TESTS_PROPERTIES(ctest_ckpt_C PROPERTIES ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/config.txt")

IF(ENABLE_FORTRAN)
	SET_SOURCE_FILES_PROPERTIES(test_ckpt.F PROPERTIES LANGUAGE Fortran)
	ADD_EXECUTABLE(test_ckpt_F test_ckpt.F)
	TARGET_LINK_LIBRARIES(test_ckpt_F scrf ${MPI_Fortran_LIBRARIES})
	ADD_TEST(NAME ctest_ckpt_F COMMAND ./test_ckpt_F)
	SET_TESTS_PROPERTIES(ctest_ckpt_F PROPERTIES ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/config.txt")
ENDIF(ENABLE_FORTRAN)

# add target: "make check"
# prints output from the ctests
ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
