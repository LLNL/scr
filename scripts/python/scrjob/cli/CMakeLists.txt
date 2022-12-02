SET(SCRCLI
  __init__.py
  scr_index.py
  scr_flush_file.py
  scr_log.py
)

FOREACH(pyscript IN ITEMS ${SCRCLI})
	CONFIGURE_FILE(${pyscript} ${pyscript} @ONLY)
ENDFOREACH(pyscript IN ITEMS ${SCRCLI})

INSTALL(FILES ${SCRCLI} DESTINATION ${CMAKE_INSTALL_BINDIR}/scrpy/scrjob/cli)
