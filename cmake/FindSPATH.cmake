# - Try to find libspath
# Once done this will define
#  SPATH_FOUND - System has libspath
#  SPATH_INCLUDE_DIRS - The libspath include directories
#  SPATH_LIBRARIES - The libraries needed to use libspath

FIND_PATH(WITH_SPATH_PREFIX
    NAMES include/spath.h
)

FIND_LIBRARY(SPATH_LIBRARIES
    NAMES spath
    HINTS ${WITH_SPATH_PREFIX}/lib
)

FIND_PATH(SPATH_INCLUDE_DIRS
    NAMES spath.h
    HINTS ${WITH_SPATH_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(SPATH DEFAULT_MSG
    SPATH_LIBRARIES
    SPATH_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	SPATH_LIBRARIES
	SPATH_INCLUDE_DIRS
)
