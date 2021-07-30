SET(PYFE
  __init__.py
  list_dir.py
  list_down_nodes.py
  parsetime.py
  postrun.py
  scr_environment.py
  scr_hostlist.py
)
INSTALL(FILES ${PYFE} DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe/pyfe)

SET(PYFE_EXEC
  scr_check_node.py
  scr_common.py
  scr_env.py
  scr_flux.py
  scr_get_jobstep_id.py
  scr_glob_hosts.py
  scr_halt.py
  scr_inspect.py
  scr_jsrun.py
  scr_kill_jobstep.py
  scr_list_dir.py
  scr_list_down_nodes.py
  scr_lrun.py
  scr_mpirun.py
  scr_param.py
  scr_postrun.py
  scr_poststage.py
  scr_prerun.py
  scr_run.py
  scr_scavenge.py
  scr_srun.py
  scr_test_runtime.py
  scr_watchdog.py
)
INSTALL(FILES ${PYFE_EXEC}
PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
        DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe/pyfe)

CONFIGURE_FILE(scr_const.py.in scr_const.py @ONLY)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/scr_const.py.in PROPERTIES GENERATED FALSE)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/scr_const.py PROPERTIES GENERATED TRUE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/scr_const.py
PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
        DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe/pyfe)

ADD_SUBDIRECTORY(cli)
ADD_SUBDIRECTORY(resmgr)
ADD_SUBDIRECTORY(joblauncher)
