SET(PYFE
  __init__.py
  list_dir.py
  list_down_nodes.py
  parsetime.py
  pdsh_clustershell.py
  postrun.py
  scr_check_node.py
  scr_common.py
  scr_env.py
  scr_environment.py
  scr_get_jobstep_id.py
  scr_glob_hosts.py
  scr_halt.py
  scr_hostlist.py
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
  setup.py
)
INSTALL(FILES ${PYFE} DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe)

CONFIGURE_FILE(scr_const.py.in scr_const.py @ONLY)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/scr_const.py.in PROPERTIES GENERATED FALSE)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/scr_const.py PROPERTIES GENERATED TRUE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/scr_const.py DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe)

ADD_SUBDIRECTORY(resmgr)
ADD_SUBDIRECTORY(joblauncher)
