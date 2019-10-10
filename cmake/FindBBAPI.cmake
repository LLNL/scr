# - Try to find libbbAPI
# Once done this will define
#  BBAPI_FOUND - System has libdatawarp
#  BBAPI_INCLUDE_DIRS - The libdatawarp include directories
#  BBAPI_LIBRARIES - The libraries needed to use libdatawarp

FIND_PATH(WITH_BBAPI_PREFIX
    NAMES include/bbapi.h
)

FIND_LIBRARY(BBAPI_LIBRARIES
    NAMES bbAPI
    HINTS ${WITH_BBAPI_PREFIX}/lib /opt/ibm/bb/lib
)

FIND_PATH(BBAPI_INCLUDE_DIRS
    NAMES bbapi.h
    HINTS ${WITH_BBAPI_PREFIX}/include /opt/ibm/bb/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(BBAPI DEFAULT_MSG
    BBAPI_LIBRARIES
    BBAPI_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	BBAPI_LIBRARIES
	BBAPI_INCLUDE_DIRS
)
