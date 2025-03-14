###############
# Build tests
###############
INCLUDE_DIRECTORIES(${PROJECT_BINARY_DIR})

LIST(APPEND axl_test_srcs
    axl_cp.c
)

ADD_EXECUTABLE(axl_cp axl_cp.c)
ADD_EXECUTABLE(axl_test_config test_config.c)

TARGET_LINK_LIBRARIES(axl_cp axl_o kvtree_o ${SCR_EXTERNAL_LIBS})
TARGET_LINK_LIBRARIES(axl_test_config axl_o kvtree_o ${SCR_EXTERNAL_LIBS})

################
# Add tests to ctest
################

CONFIGURE_FILE(test_axl.sh ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)
CONFIGURE_FILE(test_axl_metadata.sh ${CMAKE_CURRENT_BINARY_DIR} COPYONLY)

ADD_TEST(axl_sync_test test_axl.sh sync)

IF(HAVE_PTHREADS)
    ADD_TEST(axl_pthreads_test test_axl.sh pthread)
ENDIF(HAVE_PTHREADS)

ADD_TEST(axl_metadata_test test_axl_metadata.sh)

IF(BBAPI_FOUND)
    ADD_TEST(axl_bbapi_test test_axl.sh bbapi)

    # Create 300 files, cancel transfer after 3 seconds.
    ADD_TEST(axl_bbapi_cancel_test test_axl.sh -n 300 -c 3 bbapi)
ENDIF(BBAPI_FOUND)

# TODO: this test is causing an error and needs fixing
# # Create 100 files, pause the transfer after 1000 bytes or more
# # have been written, and cancel the transfer after 1 second.
# IF(HAVE_PTHREADS)
#     ADD_TEST(axl_pthreads_cancel_test test_axl.sh -n 100 -p 1000 -c 1 pthread)
# ENDIF(HAVE_PTHREADS)

# Create 100 files, pause the transfer after 1000 bytes or more
# have been written, cancel the transfer after 1 second, and resume
# the transfer.
ADD_TEST(axl_sync_resume_test test_axl.sh -n 100 -p 1000 -c 1 -U sync)

IF(HAVE_PTHREADS)
    ADD_TEST(axl_pthread_resume_test test_axl.sh -n 100 -p 1000 -c 1 -U pthread)
ENDIF(HAVE_PTHREADS)

IF(BBAPI_FOUND)
    # Create 300 files, cancel transfer after 3 seconds, resume transfer.
    # Values found through experimentation.
    ADD_TEST(axl_bbapi_resume_test test_axl.sh -n 300 -c 3 -U  bbapi)
ENDIF(BBAPI_FOUND)

ADD_TEST(axl_test_config axl_test_config)

####################
# make a verbose "test" target named "axl_check"
####################

ADD_CUSTOM_TARGET(axl_check COMMAND ${CMAKE_CTEST_COMMAND} --verbose)
