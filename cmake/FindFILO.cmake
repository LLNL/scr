# - Try to find libfilo
# Once done this will define
#  FILO_FOUND - System has libfilo
#  FILO_INCLUDE_DIRS - The libfilo include directories
#  FILO_LIBRARIES - The libraries needed to use libfilo

FIND_PATH(WITH_FILO_PREFIX
    NAMES include/filo.h
)

FIND_LIBRARY(FILO_LIBRARIES
    NAMES filo
    HINTS ${WITH_FILO_PREFIX}/lib
)

FIND_PATH(FILO_INCLUDE_DIRS
    NAMES filo.h
    HINTS ${WITH_FILO_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(FILO DEFAULT_MSG
    FILO_LIBRARIES
    FILO_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	FILO_LIBRARIES
	FILO_INCLUDE_DIRS
)
