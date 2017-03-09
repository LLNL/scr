# Install examples source code

LIST(APPEND example_files
	test_common.c
	test_common.h
	test_api.c
	test_api_file.c
	test_api_multiple.c
	test_api_multiple_file.c
	test_interpose.c
	test_interpose_multiple.c
	test_ckpt.C
	test_ckpt.F
	scr.moab
	scr_interpose.moab
	README.md
)
INSTALL(FILES ${example_files} DESTINATION share/scr/examples)

## Configure & install makefile
CONFIGURE_FILE(makefile.examples.in makefile.examples @ONLY)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_SOURCE_DIR}/makefile.examples.in PROPERTIES GENERATED FALSE)
SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/makefile.examples PROPERTIES GENERATED TRUE)
INSTALL(FILES ${CMAKE_CURRENT_BINARY_DIR}/makefile.examples DESTINATION share/scr/examples RENAME makefile)
