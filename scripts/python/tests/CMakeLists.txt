SET(SCRJOB_TESTS_TXT
  README.md
  sleeper.c
  printer.c
)
INSTALL(FILES ${SCRJOB_TESTS_TXT}
        DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/tests)

SET(SCRJOB_TESTS_EXE
  fluxenv.sh
  resetenv.sh
  runtest.sh
  test_flush_file.py
  test_launch.py
  test_log.py
  test_pdsh.py
  test_resmgr.py
  test_scr_index.py
  test_watchdog.py
)
INSTALL(FILES ${SCRJOB_TESTS_EXE}
PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
        DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python/tests)
