SET(JOBLAUNCHERS
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
INSTALL(FILES ${JOBLAUNCHERS} DESTINATION ${CMAKE_INSTALL_BINDIR}/scrpy/scrjob/launchers)
