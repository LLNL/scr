# - Try to find PDSH
# Once done this will define
#  PDSH_FOUND - System has pdsh installed
#  PDSH_EXE - Location of the pdsh binary
#  DSHBAK_EXE - Location of the dshbak binary

FIND_PATH(WITH_PDSH_PREFIX
    NAMES bin/pdsh
)

FIND_PROGRAM(PDSH_EXE
	NAMES pdsh
	HINTS ${WITH_PDSH_PREFIX}/bin
)

FIND_PROGRAM(DSHBAK_EXE
	NAMES dshbak
	HINTS ${WITH_PDSH_PREFIX}/bin
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PDSH DEFAULT_MSG
	PDSH_EXE
	DSHBAK_EXE
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	PDSH_EXE
	DSHBAK_EXE
)
