INSTALL(FILES README.md DESTINATION ${CMAKE_INSTALL_DATADIR}/scr/jobscripts)

SET(SCRRUN_SH
  scr_srun.sh
  scr_srun_loop.sh
  scr_jsrun.sh
  scr_jsrun_loop.sh
)

FOREACH(script IN ITEMS ${SCRRUN_SH})
	CONFIGURE_FILE(${script} ${script} @ONLY)
        SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/${script} PROPERTIES GENERATED TRUE)
        INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/${script}
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
                DESTINATION ${CMAKE_INSTALL_DATADIR}/scr/jobscripts)
ENDFOREACH(script IN ITEMS ${SCRRUN_SH})
