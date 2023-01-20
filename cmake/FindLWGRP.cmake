# - Try to find liblwgrp
# Once done this will define
#  LWGRP_FOUND - System has liblwgrp
#  LWGRP_INCLUDE_DIRS - The liblwgrp include directories
#  LWGRP_LIBRARIES - The libraries needed to use liblwgrp

FIND_PATH(WITH_LWGRP_PREFIX
    NAMES include/lwgrp.h
)

FIND_LIBRARY(LWGRP_LIBRARIES
    NAMES lwgrp
    HINTS ${WITH_LWGRP_PREFIX}/lib
)

FIND_PATH(LWGRP_INCLUDE_DIRS
    NAMES lwgrp.h
    HINTS ${WITH_LWGRP_PREFIX}/include
)

INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(LWGRP DEFAULT_MSG
    LWGRP_LIBRARIES
    LWGRP_INCLUDE_DIRS
)

# Hide these vars from ccmake GUI
MARK_AS_ADVANCED(
	LWGRP_LIBRARIES
	LWGRP_INCLUDE_DIRS
)
