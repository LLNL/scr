# Function to configure man pages (file.in -> file) and install into man directory
FUNCTION(SCR_INSTALL_MAN file)
	CONFIGURE_FILE(${file}.in ${file} @ONLY)
	SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/${file}.in PROPERTIES GENERATED FALSE)
	SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/${file} PROPERTIES GENERATED TRUE)

	INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/${file} DESTINATION share/man/man1)
ENDFUNCTION(SCR_INSTALL_MAN file)

LIST(APPEND man_pages
	scr.1
	scr_halt.1
	scr_index.1
	scr_postrun.1
	scr_prerun.1
	scr_srun.1
)

FOREACH(page IN ITEMS ${man_pages})
	SCR_INSTALL_MAN(${page})
ENDFOREACH(page IN ITEMS ${man_pages})
