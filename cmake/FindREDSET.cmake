# - Try to find libredset
# Once done this will define
#  REDSET_FOUND - System has libredset
#  REDSET_INCLUDE_DIRS - The libredset include directories
#  REDSET_LIBRARIES - The libraries needed to use libredset

FIND_PATH(WITH_REDSET_PREFIX
    NAMES include/redset.h
)

FIND_LIBRARY(REDSET_LIBRARIES
    NAMES redset
    HINTS ${WITH_REDSET_PREFIX}/lib
)

FIND_PATH(REDSET_INCLUDE_DIRS
    NAMES redset.h
    HINTS ${WITH_REDSET_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(REDSET DEFAULT_MSG
    REDSET_LIBRARIES
    REDSET_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	REDSET_LIBRARIES
	REDSET_INCLUDE_DIRS
)
