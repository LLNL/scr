SET(PYFE
  __init__.py
  list_dir.py
  parsetime.py
  environment.py
  hostlist.py
)

FOREACH(pyscript IN ITEMS ${PYFE})
	CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${PYFE})

INSTALL(FILES ${PYFE} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob)

SET(PYFE_EXEC
  list_down_nodes.py
  prerun.py
  postrun.py
  should_exit.py
  scrlog.py
  test_runtime.py

  scr_ckpt_interval.py
  scr_check_node.py
  scr_common.py
  scr_env.py
  scr_glob_hosts.py
  scr_inspect.py
  scr_kill_jobstep.py
  scr_list_dir.py
  scr_param.py
  scr_poststage.py
  scr_scavenge.py
  scr_watchdog.py
)

FOREACH(pyscript IN ITEMS ${PYFE_EXEC})
	CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${PYFE_EXEC})

INSTALL(FILES ${PYFE_EXEC}
PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
        DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob)

CONFIGURE_FILE(scr_const.py.in scr_const.py @ONLY)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/scr_const.py PROPERTIES GENERATED TRUE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/scr_const.py
PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
        DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob)

ADD_SUBDIRECTORY(cli)
ADD_SUBDIRECTORY(resmgrs)
ADD_SUBDIRECTORY(launchers)
