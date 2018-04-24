# - Try to find librankstr
# Once done this will define
#  RANKSTR_FOUND - System has librankstr
#  RANKSTR_INCLUDE_DIRS - The librankstr include directories
#  RANKSTR_LIBRARIES - The libraries needed to use librankstr

FIND_PATH(WITH_RANKSTR_PREFIX
    NAMES include/rankstr_mpi.h
)

FIND_LIBRARY(RANKSTR_LIBRARIES
    NAMES rankstr
    HINTS ${WITH_RANKSTR_PREFIX}/lib
)

FIND_PATH(RANKSTR_INCLUDE_DIRS
    NAMES rankstr_mpi.h
    HINTS ${WITH_RANKSTR_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(RANKSTR DEFAULT_MSG
    RANKSTR_LIBRARIES
    RANKSTR_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	RANKSTR_LIBRARIES
	RANKSTR_INCLUDE_DIRS
)
