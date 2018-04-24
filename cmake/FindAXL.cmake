# - Try to find libaxl
# Once done this will define
#  AXL_FOUND - System has libaxl
#  AXL_INCLUDE_DIRS - The libaxl include directories
#  AXL_LIBRARIES - The libraries needed to use libaxl

FIND_PATH(WITH_AXL_PREFIX
    NAMES include/axl.h
)

FIND_LIBRARY(AXL_LIBRARIES
    NAMES axl
    HINTS ${WITH_AXL_PREFIX}/lib
)

FIND_PATH(AXL_INCLUDE_DIRS
    NAMES axl.h
    HINTS ${WITH_AXL_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(AXL DEFAULT_MSG
    AXL_LIBRARIES
    AXL_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	AXL_LIBRARIES
	AXL_INCLUDE_DIRS
)
