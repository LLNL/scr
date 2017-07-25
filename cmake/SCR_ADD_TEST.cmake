FUNCTION(SCR_ADD_TEST name args outputs)
	ADD_TEST(NAME ${name}_start COMMAND ./${name} ${args})
	SET_PROPERTY(TEST ${name}_start APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")

	ADD_TEST(NAME ${name}_restart COMMAND ./${name} ${args})
	SET_PROPERTY(TEST ${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")
	SET_PROPERTY(TEST ${name}_restart APPEND PROPERTY DEPENDS start_${name})

	# Note: cleanup scripts automatically cleans /tmp and .scr
	ADD_TEST(NAME ${name}_cleanup COMMAND ./cleanup.sh ${outputs} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
	SET_PROPERTY(TEST ${name}_cleanup APPEND PROPERTY DEPENDS restart_${name})

ENDFUNCTION(SCR_ADD_TEST name args outputs)
