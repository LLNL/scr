SET(JOBLAUNCHERS
  README.md
  __init__.py
  aprun.py
  auto.py
  joblauncher.py
  jsrun.py
  lrun.py
  mpirun.py
  srun.py
  flux.py
)

FOREACH(pyscript IN ITEMS ${JOBLAUNCHERS})
	CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${JOBLAUNCHERS})

INSTALL(FILES ${JOBLAUNCHERS} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob/launchers)
