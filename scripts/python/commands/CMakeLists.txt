SET(PYFE_BIN
  scr_halt
  scr_list_down_nodes
  scr_prerun
  scr_postrun
  scr_should_exit

#  scr_run
#  scr_srun
#  scr_jsrun
#  scr_lrun
#  scr_flux
#  scr_mpirun
)

FOREACH(pyscript IN ITEMS ${PYFE_BIN})
	CONFIGURE_FILE(${pyscript}.py ${pyscript} @ONLY)
        SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/${pyscript} PROPERTIES GENERATED TRUE)
        INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/${pyscript}
        PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
                DESTINATION ${CMAKE_INSTALL_BINDIR})
ENDFOREACH(pyscript IN ITEMS ${PYFE_BIN})
