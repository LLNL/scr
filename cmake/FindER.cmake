# - Try to find liber
# Once done this will define
#  ER_FOUND - System has liber
#  ER_INCLUDE_DIRS - The liber include directories
#  ER_LIBRARIES - The libraries needed to use liber

FIND_PATH(WITH_ER_PREFIX
    NAMES include/er.h
)

FIND_LIBRARY(ER_LIBRARIES
    NAMES er
    HINTS ${WITH_ER_PREFIX}/lib
)

FIND_PATH(ER_INCLUDE_DIRS
    NAMES er.h
    HINTS ${WITH_ER_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(ER DEFAULT_MSG
    ER_LIBRARIES
    ER_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	ER_LIBRARIES
	ER_INCLUDE_DIRS
)
