SET(SCRIPTS
  README.md
  __init__.py
  remoteexec.py

  pdsh.py
  clustershell.py
  flux.py
)

FOREACH(pyscript IN ITEMS ${SCRIPTS})
	CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${SCRIPTS})

INSTALL(FILES ${SCRIPTS} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob/remoteexec)
