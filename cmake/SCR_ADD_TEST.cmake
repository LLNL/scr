FUNCTION(SCR_LAUNCHER_PARMS procs)
    IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
        SET(test_launcher "mpirun" PARENT_SCOPE)
        SET(test_param "-np ${procs} ${MPIRUN_FLAGS}" PARENT_SCOPE)
    ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "SLURM")
        SET(test_launcher "srun" PARENT_SCOPE)
        SET(test_param "-t 5 -N ${procs} -n ${procs}" PARENT_SCOPE)
    ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "LSF")
        SET(test_launcher "jsrun" PARENT_SCOPE)
        SET(test_param "--nrs ${procs} -r 1" PARENT_SCOPE)
    ELSEIF(${SCR_RESOURCE_MANAGER} STREQUAL "FLUX")
        SET(test_launcher "flux" PARENT_SCOPE)
        SET(test_param "run --nodes ${procs} --ntasks ${procs}" PARENT_SCOPE)
    ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
ENDFUNCTION(SCR_LAUNCHER_PARMS procs)

FUNCTION(SCR_LAUNCHER_JOBID testname)
    IF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
        SET_PROPERTY(TEST ${testname} APPEND PROPERTY ENVIRONMENT "SCR_JOB_ID=439")
    ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "NONE")
ENDFUNCTION(SCR_LAUNCHER_JOBID name)

FUNCTION(SCR_ADD_TEST name args outputs)
    IF(${SCR_RESOURCE_MANAGER} STREQUAL "FLUX")
        # Use python scripting framework for testing flux, it has no bash scripts
        # Single process tests
        SCR_LAUNCHER_PARMS(1)
        ADD_TEST(NAME serial_${name}_restart_py COMMAND run_test_py.sh ${test_launcher} ${test_param} ./${name} restart ${args})
        SCR_LAUNCHER_JOBID("serial_${name}_restart_py")

        # Multi-process, multi-node tests
        SCR_LAUNCHER_PARMS(4)
        ADD_TEST(NAME parallel_${name}_restart_py COMMAND run_test_py.sh ${test_launcher} ${test_param} ./${name} restart ${args})
        SCR_LAUNCHER_JOBID("parallel_${name}_restart_py")
    ELSE(${SCR_RESOURCE_MANAGER} STREQUAL "FLUX")
        # Single process tests
        SCR_LAUNCHER_PARMS(1)
        ADD_TEST(NAME serial_${name}_restart COMMAND run_test.sh ${test_launcher} ${test_param} ./${name} restart ${args})
        SCR_LAUNCHER_JOBID("serial_${name}_restart")

        # Multi-process, multi-node tests
        SCR_LAUNCHER_PARMS(4)
        ADD_TEST(NAME parallel_${name}_restart COMMAND run_test.sh ${test_launcher} ${test_param} ./${name} restart ${args})
        SCR_LAUNCHER_JOBID("parallel_${name}_restart")
    ENDIF(${SCR_RESOURCE_MANAGER} STREQUAL "FLUX")

ENDFUNCTION(SCR_ADD_TEST name args outputs)
