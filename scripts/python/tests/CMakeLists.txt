SET(PYFE_TESTS_TXT
  README.rst
  sleeper.c
  printer.c
)
INSTALL(FILES ${PYFE_TESTS_TXT}
        DESTINATION ${CMAKE_INSTALL_BINDIR}/scrpy/tests)

SET(PYFE_TESTS_EXE
  fluxenv.sh
  resetenv.sh
  runtest.sh
  test_flush_file.py
  test_launch.py
  test_log.py
  test_pdsh.py
  test_rm.py
  test_scr_index.py
  test_watchdog.py
)
INSTALL(FILES ${PYFE_TESTS_EXE}
PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
        DESTINATION ${CMAKE_INSTALL_BINDIR}/scrpy/tests)
