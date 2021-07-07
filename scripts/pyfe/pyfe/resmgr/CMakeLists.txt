SET(RESOURCEMANAGERS
  __init__.py
  aprun.py
  auto.py
  lsf.py
  nodetests.py
  pmix.py
  resourcemanager.py
  slurm.py
)
INSTALL(FILES ${RESOURCEMANAGERS} DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe/resmgr)
