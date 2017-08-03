FUNCTION(SCR_ADD_PARALLEL_TEST name nodes args outputs)
	SET(test_param -N${nodes} -ppdebug "./${name}")

	ADD_TEST(NAME ${name}_start_parallel COMMAND "srun" ${test_param} ${args})
	SET_PROPERTY(TEST ${name}_start_parallel APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")

	ADD_TEST(NAME ${name}_restart_parallel COMMAND "srun" ${test_param} ${args})
	SET_PROPERTY(TEST ${name}_restart_parallel APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")
	SET_PROPERTY(TEST ${name}_restart_parallel APPEND PROPERTY DEPENDS ${name}_start_parallel)

	# Note: cleanup scripts automatically cleans /tmp and .scr
	ADD_TEST(NAME ${name}_cleanup_parallel COMMAND ./test_cleanup.sh ${outputs} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
	SET_PROPERTY(TEST ${name}_cleanup_parallel APPEND PROPERTY DEPENDS ${name}_restart_parallel)

ENDFUNCTION(SCR_ADD_PARALLEL_TEST name nodes args outputs)
