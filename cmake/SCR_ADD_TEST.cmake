FUNCTION(SCR_ADD_TEST name args outputs)
	ADD_TEST(NAME serial_${name}_start COMMAND ./${name} ${args})
	SET_PROPERTY(TEST serial_${name}_start APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")

	ADD_TEST(NAME serial_${name}_restart COMMAND ./${name} ${args})
	SET_PROPERTY(TEST serial_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")
	SET_PROPERTY(TEST serial_${name}_restart APPEND PROPERTY DEPENDS serial_${name}_start)

	IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
		SET_PROPERTY(TEST serial_${name}_start APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
		SET_PROPERTY(TEST serial_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
	ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

	# Note: cleanup scripts automatically cleans /tmp and .scr
	ADD_TEST(NAME serial_${name}_cleanup COMMAND ./test_cleanup.sh ${outputs} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
	SET_PROPERTY(TEST serial_${name}_cleanup APPEND PROPERTY DEPENDS serial_${name}_restart)

ENDFUNCTION(SCR_ADD_TEST name args outputs)
