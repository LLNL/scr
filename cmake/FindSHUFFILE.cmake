# - Try to find libshuffile
# Once done this will define
#  SHUFFILE_FOUND - System has libshuffile
#  SHUFFILE_INCLUDE_DIRS - The libshuffile include directories
#  SHUFFILE_LIBRARIES - The libraries needed to use libshuffile

FIND_PATH(WITH_SHUFFILE_PREFIX
    NAMES include/shuffile.h
)

FIND_LIBRARY(SHUFFILE_LIBRARIES
    NAMES shuffile
    HINTS ${WITH_SHUFFILE_PREFIX}/lib
)

FIND_PATH(SHUFFILE_INCLUDE_DIRS
    NAMES shuffile.h
    HINTS ${WITH_SHUFFILE_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(SHUFFILE DEFAULT_MSG
    SHUFFILE_LIBRARIES
    SHUFFILE_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	SHUFFILE_LIBRARIES
	SHUFFILE_INCLUDE_DIRS
)
