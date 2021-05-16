# - Try to find libyogrt
# Once done this will define
#  YOGRT_FOUND - System has libyogrt
#  YOGRT_INCLUDE_DIRS - The libyogrt include directories
#  YOGRT_LIBRARIES - The libraries needed to use libyogrt

FIND_PATH(WITH_YOGRT_PREFIX
    NAMES include/yogrt.h
)

FIND_LIBRARY(YOGRT_LIBRARIES
    NAMES yogrt
    HINTS ${WITH_YOGRT_PREFIX}/lib
)

FIND_PATH(YOGRT_INCLUDE_DIRS
    NAMES yogrt.h
    HINTS ${WITH_YOGRT_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(YOGRT DEFAULT_MSG
    YOGRT_LIBRARIES
    YOGRT_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	YOGRT_LIBRARIES
	YOGRT_INCLUDE_DIRS
)
