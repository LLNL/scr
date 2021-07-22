SET(SCRCLI
  __init__.py
  scr_index.py
  scr_flush_file.py
)
INSTALL(FILES ${SCRCLI} DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe/pyfe/cli)
