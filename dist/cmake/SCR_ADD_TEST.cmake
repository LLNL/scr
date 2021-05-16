FUNCTION(SCR_ADD_TEST name args outputs)

  SET(s_nodes "1")
  IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
    SET(test_param mpirun -np ${s_nodes})
  ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "SLURM")
    SET(test_param srun -ppbatch -t 5 -N ${s_nodes} ) 
  ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "LSF")
    SET(test_param lrun -N ${s_nodes} -T 1 -v ) 
  ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

	# Serial Tests
	ADD_TEST(NAME serial_${name}_start COMMAND ${test_param} run_test.sh ./${name} start ${args})
	SET_PROPERTY(TEST serial_${name}_start APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")

	ADD_TEST(NAME serial_${name}_restart COMMAND ${test_param} run_test.sh ./${name} restart ${args})
	SET_PROPERTY(TEST serial_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")
	SET_PROPERTY(TEST serial_${name}_restart APPEND PROPERTY DEPENDS serial_${name}_start)

	IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
		SET_PROPERTY(TEST serial_${name}_start APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
		SET_PROPERTY(TEST serial_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
	ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

	# Note: cleanup scripts automatically cleans /tmp and .scr
#	ADD_TEST(NAME serial_${name}_precleanup COMMAND ./test_cleanup.sh ${outputs} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
#	SET_PROPERTY(TEST serial_${name}_start APPEND PROPERTY DEPENDS serial_${name}_precleanup)

	# Parallel Tests
	SET(p_nodes "4")
	IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
		SET(test_param mpirun -np ${p_nodes})
	ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "SLURM")
		SET(test_param srun -ppbatch -t 5 -N ${p_nodes} -n ${p_nodes})
	ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "LSF")
		SET(test_param lrun -N ${p_nodes} -T 1 -v)
	ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

	ADD_TEST(NAME parallel_${name}_start COMMAND ${test_param} run_test.sh ./${name} start ${args})
	SET_PROPERTY(TEST parallel_${name}_start APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")

	ADD_TEST(NAME parallel_${name}_restart COMMAND ${test_param} run_test.sh ./${name} restart ${args})
	SET_PROPERTY(TEST parallel_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_CONF_FILE=${CMAKE_CURRENT_SOURCE_DIR}/test.conf")
	SET_PROPERTY(TEST parallel_${name}_restart APPEND PROPERTY DEPENDS parallel_${name}_start)

	IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
		SET_PROPERTY(TEST parallel_${name}_start APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
		SET_PROPERTY(TEST parallel_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
	ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

	# Note: cleanup scripts automatically cleans /tmp and .scr
#	ADD_TEST(NAME parallel_${name}_precleanup COMMAND ./test_cleanup.sh ${outputs} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
#	SET_PROPERTY(TEST parallel_${name}_start APPEND PROPERTY DEPENDS parallel_${name}_precleanup)

ENDFUNCTION(SCR_ADD_TEST name args outputs)
