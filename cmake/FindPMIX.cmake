# - Try to find libpmix
# Once done this will define
#  PMIX_FOUND - System has libpmix
#  PMIX_INCLUDE_DIRS - The libpmix include directories
#  PMIX_LIBRARIES - The libraries needed to use libpmix

FIND_PATH(WITH_PMIX_PREFIX
    NAMES include/pmix.h
)

FIND_LIBRARY(PMIX_LIBRARIES
    NAMES pmix
    HINTS ${WITH_PMIX_PREFIX}/lib
)

FIND_PATH(PMIX_INCLUDE_DIRS
    NAMES pmix.h
    HINTS ${WITH_PMIX_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(PMIX DEFAULT_MSG
    PMIX_LIBRARIES
    PMIX_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	PMIX_LIBRARIES
	PMIX_INCLUDE_DIRS
)
