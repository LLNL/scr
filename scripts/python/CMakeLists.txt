SET(SCRJOB_TOP
  README.md
  LICENSE
#  MANIFEST.in
#  setup.py
)

INSTALL(FILES ${SCRJOB_TOP} DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python)

SET(SCRJOB_EXEC
  scr_ckpt_interval.py
  scr_check_node.py
  scr_env.py
  scr_hostlist.py
  scr_kill_jobstep.py
  scr_list_dir.py
  scr_poststage.py
  scr_scavenge.py
)

FOREACH(pyscript IN ITEMS ${SCRJOB_EXEC})
        CONFIGURE_FILE(${pyscript} ${pyscript} COPYONLY)
ENDFOREACH(pyscript IN ITEMS ${SCRJOB_EXEC})

INSTALL(FILES ${SCRJOB_EXEC}
        DESTINATION ${CMAKE_INSTALL_LIBEXECDIR}/python)

ADD_SUBDIRECTORY(scrjob)
ADD_SUBDIRECTORY(commands)
ADD_SUBDIRECTORY(tests)
