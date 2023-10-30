SET(NODETESTS
  README.md
  __init__.py
  nodetests.py
  nodetest.py

  scr_exclude_nodes.py
  resmgr.py
  ping.py
  echo.py
  dir_capacity.py
)

FOREACH(pyscript IN ITEMS ${NODETESTS})
	CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${NODETESTS})

INSTALL(FILES ${NODETESTS} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob/nodetests)
