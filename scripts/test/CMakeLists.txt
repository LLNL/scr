# Copy test script and register the test

FILE(COPY ${CMAKE_CURRENT_SOURCE_DIR}/test_scr_param DESTINATION ${CMAKE_CURRENT_BINARY_DIR})

ADD_TEST(NAME test_scr_param  COMMAND ./test_scr_param)
SET_PROPERTY(TEST test_scr_param APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")
