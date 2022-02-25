SET(SCRCLI
  __init__.py
  scr_index.py
  scr_flush_file.py
  scr_log.py
)
INSTALL(FILES ${SCRCLI} DESTINATION ${CMAKE_INSTALL_BINDIR}/scrpy/scrjob/cli)
