# Install examples source code

LIST(APPEND example_files
	test_common.c
	test_common.h
	test_api.c
	test_api_multiple.c
	test_ckpt.cpp
	test_ckpt.F
	test_ckpt.F90
	test_config.c
	README.md
)
INSTALL(FILES ${example_files} DESTINATION ${CMAKE_INSTALL_DATADIR}/scr/examples)

## static linking means we must pass a link line to our examples
IF(SCR_LINK_STATIC)
    IF(NOT "${SCR_LINK_LINE}" STREQUAL "")
        STRING(REPLACE ";" " " SCR_LINK_LINE ${SCR_LINK_LINE})
    ENDIF(NOT "${SCR_LINK_LINE}" STREQUAL "")
ELSE(SCR_LINK_STATIC)
    SET(SCR_LINK_LINE)
ENDIF(SCR_LINK_STATIC)

## Configure & install makefile
CONFIGURE_FILE(makefile.examples.in makefile.examples @ONLY)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/makefile.examples PROPERTIES GENERATED TRUE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/makefile.examples DESTINATION ${CMAKE_INSTALL_DATADIR}/scr/examples RENAME makefile)

# CMAKE PROPER FOR EXAMPLES

## Prefer static?
IF(SCR_LINK_STATIC)
        SET(SCR_LINK_TO scr::scr-static)
ELSE(SCR_LINK_STATIC)
        SET(SCR_LINK_TO scr::scr)
ENDIF(SCR_LINK_STATIC)


INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_BINARY_DIR})

## Move test cleanup script to build directory
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_cleanup.sh DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

## Move test wrapper script
## Each test needs the wrapper script so it can do the cleanup as part of the test, to ensure it gets
## run on the same parallel nodes
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/run_test.sh DESTINATION ${CMAKE_CURRENT_BINARY_DIR})
FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/run_test_py.sh DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

ADD_EXECUTABLE(test_api test_common.c test_api.c)
TARGET_LINK_LIBRARIES(test_api PRIVATE ${SCR_LINK_TO})
SCR_ADD_TEST(test_api "" "ckpt.*")

ADD_EXECUTABLE(test_api_shared_file test_common.c test_api.c)
TARGET_LINK_LIBRARIES(test_api_shared_file PRIVATE ${SCR_LINK_TO})
SCR_ADD_TEST(test_api_shared_file "--shared" "ckpt.*")

ADD_EXECUTABLE(test_config test_config.c)
TARGET_LINK_LIBRARIES(test_config PRIVATE ${SCR_LINK_TO})
SCR_ADD_TEST(test_config "" "test_config.d")

#ADD_EXECUTABLE(test_api_file test_common.c test_api_file.c)
#TARGET_LINK_LIBRARIES(test_api_file ${SCR_LINK_TO})
#SCR_ADD_TEST: proper usage is unknown

ADD_EXECUTABLE(test_api_multiple test_common.c test_api_multiple.c)
TARGET_LINK_LIBRARIES(test_api_multiple PRIVATE ${SCR_LINK_TO})
SCR_ADD_TEST(test_api_multiple "" "")

#ADD_EXECUTABLE(test_api_multiple_file test_common.c test_api_multiple_file.c)
#TARGET_LINK_LIBRARIES(test_api_multiple_file ${SCR_LINK_TO})
#SCR_ADD_TEST: proper usage is unknown

ADD_EXECUTABLE(test_ckpt test_ckpt.cpp)
IF(MPI_CXX_FOUND)
    TARGET_INCLUDE_DIRECTORIES(test_ckpt PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${MPI_CXX_INCLUDE_PATH})
    TARGET_LINK_LIBRARIES(test_ckpt PRIVATE ${SCR_LINK_TO} ${MPI_CXX_LIBRARIES})
ELSE(MPI_CXX_FOUND)
    TARGET_LINK_LIBRARIES(test_ckpt PRIVATE ${SCR_LINK_TO})
ENDIF(MPI_CXX_FOUND)
SCR_ADD_TEST(test_ckpt "" "rank_0.ckpt")

IF(ENABLE_FORTRAN)
        SET_SOURCE_FILES_PROPERTIES(test_ckpt.F PROPERTIES LANGUAGE Fortran)
	    ADD_EXECUTABLE(test_ckpt_F test_ckpt.F)
        SET_TARGET_PROPERTIES(test_ckpt_F PROPERTIES LINKER_LANGUAGE Fortran)

	SET_SOURCE_FILES_PROPERTIES(test_ckpt.F90 PROPERTIES LANGUAGE Fortran)
	    ADD_EXECUTABLE(test_ckpt_F90 test_ckpt.F90)
        SET_TARGET_PROPERTIES(test_ckpt_F90 PROPERTIES LINKER_LANGUAGE Fortran)

        IF(SCR_LINK_STATIC)
                # needed for LANL cray
                SET(CMAKE_SHARED_LIBRARY_LINK_Fortran_FLAGS)
                TARGET_LINK_LIBRARIES(test_ckpt_F PRIVATE scr::scrf-static ${MPI_Fortran_LIBRARIES})
                TARGET_LINK_LIBRARIES(test_ckpt_F90 PRIVATE scr::scrf-static ${MPI_Fortran_LIBRARIES})
        ELSE(SCR_LINK_STATIC)
                TARGET_LINK_LIBRARIES(test_ckpt_F PRIVATE scr::scrf ${MPI_Fortran_LIBRARIES})
                TARGET_LINK_LIBRARIES(test_ckpt_F90 PRIVATE scr::scrf ${MPI_Fortran_LIBRARIES})
        ENDIF(SCR_LINK_STATIC)

	SCR_ADD_TEST(test_ckpt_F "" "rank_00000.ckpt")
	SCR_ADD_TEST(test_ckpt_F90 "" "rank_00000.ckpt")
ENDIF(ENABLE_FORTRAN)

# add target: "make check"
# prints output from the ctests
ADD_CUSTOM_TARGET(check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
