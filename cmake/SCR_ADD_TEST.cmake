FUNCTION(SCR_ADD_TEST name args outputs)

    # Single process tests
    SET(s_nodes "1")
    IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
        SET(test_param "mpirun -np ${s_nodes}")
    ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "SLURM")
        SET(test_param "srun -t 5 -N ${s_nodes}")
    ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "LSF")
        SET(test_param "jsrun --nrs ${s_nodes} -r 1")
    ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

    ADD_TEST(NAME serial_${name}_restart COMMAND run_test.sh ${test_param} ./${name} restart ${args})

    IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
        SET_PROPERTY(TEST serial_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
    ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

    # Multi-process, multi-node tests
    SET(p_nodes "4")
    IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
        SET(test_param "mpirun -np ${p_nodes}")
    ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "SLURM")
        SET(test_param "srun -t 5 -N ${p_nodes} -n ${p_nodes}")
    ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "LSF")
        SET(test_param "jsrun --nrs ${p_nodes} -r 1")
    ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

    ADD_TEST(NAME parallel_${name}_restart COMMAND run_test.sh ${test_param} ./${name} restart ${args})

    IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
        SET_PROPERTY(TEST parallel_${name}_restart APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
    ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")

ENDFUNCTION(SCR_ADD_TEST name args outputs)
