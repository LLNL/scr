# - Try to find libkvtree
# Once done this will define
#  KVTREE_FOUND - System has libkvtree
#  KVTREE_INCLUDE_DIRS - The libkvtree include directories
#  KVTREE_LIBRARIES - The libraries needed to use libkvtree

FIND_PATH(WITH_KVTREE_PREFIX
    NAMES include/kvtree.h
)

FIND_LIBRARY(KVTREE_LIBRARIES
    NAMES kvtree
    HINTS ${WITH_KVTREE_PREFIX}/lib
)

FIND_PATH(KVTREE_INCLUDE_DIRS
    NAMES kvtree.h
    HINTS ${WITH_KVTREE_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(KVTREE DEFAULT_MSG
    KVTREE_LIBRARIES
    KVTREE_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	KVTREE_LIBRARIES
	KVTREE_INCLUDE_DIRS
)
