SET(SCRJOB
  README.md
  __init__.py
  common.py
  hostlist.py
  jobenv.py
  list_down_nodes.py
  param.py
  parsetime.py
  prerun.py
  postrun.py
  run.py
  scavenge.py
  scrlog.py
  should_exit.py
  test_runtime.py
  watchdog.py
)

FOREACH(pyscript IN ITEMS ${SCRJOB})
	CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${SCRJOB})

INSTALL(FILES ${SCRJOB} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob)

CONFIGURE_FILE(config.py.in config.py @ONLY)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/config.py PROPERTIES GENERATED TRUE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/config.py
        DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob)

ADD_SUBDIRECTORY(cli)
ADD_SUBDIRECTORY(launchers)
ADD_SUBDIRECTORY(nodetests)
ADD_SUBDIRECTORY(remoteexec)
ADD_SUBDIRECTORY(resmgrs)
