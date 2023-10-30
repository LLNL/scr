SET(NODETESTS
  README.md
  __init__.py
  nodetests.py
)

FOREACH(pyscript IN ITEMS ${NODETESTS})
	CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${NODETESTS})

INSTALL(FILES ${NODETESTS} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob/nodetests)
