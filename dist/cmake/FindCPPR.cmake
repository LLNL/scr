# - Try to find libcppr
# Once done this will define
#  CPPR_FOUND - System has libcppr
#  CPPR_INCLUDE_DIRS - The libcppr include directories
#  CPPR_LIBRARIES - The libraries needed to use libcppr

FIND_PATH(WITH_CPPR_PREFIX
    NAMES include/cppr.h
)

FIND_LIBRARY(CPPR_LIBRARIES
    NAMES cppr
    HINTS ${WITH_CPPR_PREFIX}/lib
)

FIND_PATH(CPPR_INCLUDE_DIRS
    NAMES cppr.h
    HINTS ${WITH_CPPR_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CPPR DEFAULT_MSG
    CPPR_LIBRARIES
    CPPR_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	CPPR_LIBRARIES
	CPPR_INCLUDE_DIRS
)
