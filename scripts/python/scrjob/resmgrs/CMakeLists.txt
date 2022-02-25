SET(RESOURCEMANAGERS
  __init__.py
  auto.py
  flux.py
  lsf.py
  nodetests.py
  pbsalps.py
  pmix.py
  resourcemanager.py
  slurm.py
)
INSTALL(FILES ${RESOURCEMANAGERS} DESTINATION ${CMAKE_INSTALL_BINDIR}/scrpy/scrjob/resmgrs)
