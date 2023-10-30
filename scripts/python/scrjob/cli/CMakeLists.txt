SET(SCRCLI
  README.md
  __init__.py
  scr_flush_file.py
  scr_halt_cntl.py
  scr_index.py
  scr_log.py
  scr_nodes_file.py
  scr_retries_halt.py
)

FOREACH(pyscript IN ITEMS ${SCRCLI})
	CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${SCRCLI})

INSTALL(FILES ${SCRCLI} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/scrjob/cli)
