FUNCTION(SCR_ADD_PARALLEL_TEST name nodes args outputs)
	# SET(test_param -N${nodes} -ppdebug "./${name}")

	IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
		SET(test_param mpirun -np ${nodes})
	ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "SLURM")
		SET(test_param srun -ppdebug -t 5 -N ${nodes} -n ${nodes})
	ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

	ADD_TEST(NAME parallel_${name}_start COMMAND ${test_param} ./${name} ${args})
	SET_PROPERTY(TEST parallel_${name}_start APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")

	ADD_TEST(NAME parallel_${name}_restart COMMAND ${test_param} ./${name} ${args})
	SET_PROPERTY(TEST parallel_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")
	SET_PROPERTY(TEST parallel_${name}_restart APPEND PROPERTY DEPENDS parallel_${name}_start)

	IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
		SET_PROPERTY(TEST parallel_${name}_start APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
		SET_PROPERTY(TEST parallel_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
	ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

	# Note: cleanup scripts automatically cleans /tmp and .scr
	ADD_TEST(NAME parallel_${name}_cleanup COMMAND ./test_cleanup.sh ${outputs} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
	SET_PROPERTY(TEST parallel_${name}_cleanup APPEND PROPERTY DEPENDS parallel_${name}_restart)

ENDFUNCTION(SCR_ADD_PARALLEL_TEST name nodes args outputs)
