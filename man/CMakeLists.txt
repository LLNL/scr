LIST(APPEND man_pages
	scr.1
	scr_halt.1
	scr_index.1
	scr_postrun.1
	scr_prerun.1
	scr_srun.1
)

INSTALL(FILES ${man_pages} DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
