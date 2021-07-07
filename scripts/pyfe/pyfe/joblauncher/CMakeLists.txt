SET(JOBLAUNCHERS
  __init__.py
  scr_joblauncher.py
  scr_joblauncher_aprun.py
  scr_joblauncher_base.py
  scr_joblauncher_jsrun.py
  scr_joblauncher_lrun.py
  scr_joblauncher_mpirun.py
  scr_joblauncher_srun.py
)
INSTALL(FILES ${JOBLAUNCHERS} DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe/joblauncher)
