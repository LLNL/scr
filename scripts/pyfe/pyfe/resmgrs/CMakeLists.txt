SET(RESOURCEMANAGERS
  __init__.py
  auto.py
  lsf.py
  nodetests.py
  pbsalps.py
  pmix.py
  resourcemanager.py
  slurm.py
)
INSTALL(FILES ${RESOURCEMANAGERS} DESTINATION ${CMAKE_INSTALL_BINDIR}/pyfe/pyfe/resmgrs)
