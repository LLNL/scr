PROJECT(SCR)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

# Find Packages & Files
LIST(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

# process SCR CMake options
INCLUDE(SCR_OPTIONS)

## HEADERS
INCLUDE(CheckIncludeFile)

## AXL
FIND_PACKAGE(AXL REQUIRED)
IF(AXL_FOUND)
	INCLUDE_DIRECTORIES(${AXL_INCLUDE_DIRS})
	LIST(APPEND SCR_EXTERNAL_LIBS ${AXL_LIBRARIES})
	LIST(APPEND SCR_EXTERNAL_SERIAL_LIBS ${AXL_LIBRARIES})
	LIST(APPEND SCR_LINK_LINE "-laxl")
ENDIF(AXL_FOUND)

## ER
FIND_PACKAGE(ER REQUIRED)
IF(ER_FOUND)
	INCLUDE_DIRECTORIES(${ER_INCLUDE_DIRS})
	LIST(APPEND SCR_EXTERNAL_LIBS ${ER_LIBRARIES})
	LIST(APPEND SCR_LINK_LINE "-ler")
ENDIF(ER_FOUND)

## SHUFFILE
FIND_PACKAGE(SHUFFILE REQUIRED)
IF(SHUFFILE_FOUND)
	INCLUDE_DIRECTORIES(${SHUFFILE_INCLUDE_DIRS})
	LIST(APPEND SCR_EXTERNAL_LIBS ${SHUFFILE_LIBRARIES})
	LIST(APPEND SCR_LINK_LINE "-lshuffile")
ENDIF(SHUFFILE_FOUND)

## REDSET
FIND_PACKAGE(REDSET REQUIRED)
IF(REDSET_FOUND)
	INCLUDE_DIRECTORIES(${REDSET_INCLUDE_DIRS})
	LIST(APPEND SCR_EXTERNAL_LIBS ${REDSET_LIBRARIES})
	LIST(APPEND SCR_EXTERNAL_SERIAL_LIBS ${REDSET_BASE_LIBRARIES})
	LIST(APPEND SCR_LINK_LINE "-lredset")
ENDIF(REDSET_FOUND)

## SPATH
FIND_PACKAGE(SPATH REQUIRED)
IF(SPATH_FOUND)
	INCLUDE_DIRECTORIES(${SPATH_INCLUDE_DIRS})
	LIST(APPEND SCR_EXTERNAL_LIBS ${SPATH_LIBRARIES})
	LIST(APPEND SCR_EXTERNAL_SERIAL_LIBS ${SPATH_BASE_LIBRARIES})
	LIST(APPEND SCR_LINK_LINE "-lspath")
ENDIF(SPATH_FOUND)

## RankStr
FIND_PACKAGE(RANKSTR REQUIRED)
IF(RANKSTR_FOUND)
	INCLUDE_DIRECTORIES(${RANKSTR_INCLUDE_DIRS})
	LIST(APPEND SCR_EXTERNAL_LIBS ${RANKSTR_LIBRARIES})
	LIST(APPEND SCR_LINK_LINE "-lrankstr")
ENDIF(RANKSTR_FOUND)

## KVTREE
FIND_PACKAGE(KVTREE REQUIRED)
IF(KVTREE_FOUND)
	INCLUDE_DIRECTORIES(${KVTREE_INCLUDE_DIRS})
	LIST(APPEND SCR_EXTERNAL_LIBS ${KVTREE_LIBRARIES})
	LIST(APPEND SCR_EXTERNAL_SERIAL_LIBS ${KVTREE_BASE_LIBRARIES})
	LIST(APPEND SCR_LINK_LINE "-lkvtree")
ENDIF(KVTREE_FOUND)

## libdtcmp
FIND_PACKAGE(DTCMP REQUIRED)
IF(DTCMP_FOUND)
        SET(HAVE_LIBDTCMP TRUE)
        INCLUDE_DIRECTORIES(${DTCMP_INCLUDE_DIRS})
        LIST(APPEND SCR_EXTERNAL_LIBS ${DTCMP_LIBRARIES})
        LIST(APPEND SCR_LINK_LINE " -L${WITH_DTCMP_PREFIX}/lib -ldtcmp")
ENDIF(DTCMP_FOUND)

INCLUDE(SCR_DEPENDENCIES)

INCLUDE(GNUInstallDirs)
## Use X_ variable names for CLI scripts
## could use CMAKE_INSTALL_FULL_ names instead
SET(X_BINDIR ${CMAKE_INSTALL_FULL_BINDIR} CACHE INTERNAL "bin")
SET(X_DATADIR ${CMAKE_INSTALL_FULL_DATADIR} CACHE INTERNAL "share")
SET(X_INCLUDEDIR ${CMAKE_INSTALL_FULL_INCLUDEDIR} CACHE INTERNAL "include")
SET(X_LIBDIR ${CMAKE_INSTALL_FULL_LIBDIR} CACHE INTERNAL "lib")

# Generate config.h with all our build #defs
CONFIGURE_FILE(${PROJECT_SOURCE_DIR}/cmake/config.h.in ${PROJECT_BINARY_DIR}/config.h)
SET_SOURCE_FILES_PROPERTIES(config.h.in PROPERTIES GENERATED FALSE)
SET_SOURCE_FILES_PROPERTIES(${PROJECT_BINARY_DIR}/config.h PROPERTIES GENERATED TRUE)

# Subdirectories

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_SOURCE_DIR}/src)

INCLUDE(SCR_ADD_TEST)
IF(ENABLE_TESTS)
   INCLUDE(CTest)
ENDIF(ENABLE_TESTS)

ADD_SUBDIRECTORY(man)
ADD_SUBDIRECTORY(scripts)
ADD_SUBDIRECTORY(src)
ADD_SUBDIRECTORY(python)
IF(ENABLE_EXAMPLES)
   ADD_SUBDIRECTORY(examples)
ENDIF()

# Special Install Files
INSTALL(FILES README.md DESTINATION ${CMAKE_INSTALL_DATADIR}/scr)

# Package
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Scalable Checkpoint / Restart Library")
SET(CPACK_PACKAGE_VENDOR "Lawrence Livermore National Laboratory")
SET(CPACK_PACKAGE_DESCRIPTION_FILE "${PROJECT_SOURCE_DIR}/README.md")
SET(CPACK_RESOURCE_FILE_LICENSE "${PROJECT_SOURCE_DIR}/LICENSE.TXT")
# SET(CPACK_PACKAGE_VERSION_MAJOR ${SCR_MAJOR_VERSION})
# SET(CPACK_PACKAGE_VERSION_MINOR ${SCR_MINOR_VERSION})
# SET(CPACK_PACKAGE_VERSION_PATCH ${SCR_PATCH_VERSION})
# SET(CPACK_PACKAGE_INSTALL_DIRECTORY "CMake ${CMake_VERSION_MAJOR}.${CMake_VERSION_MINOR}")
# IF(WIN32 AND NOT UNIX)
#   # There is a bug in NSI that does not handle full unix paths properly. Make
#   # sure there is at least one set of four (4) backlasshes.
#   SET(CPACK_PACKAGE_ICON "${CMake_SOURCE_DIR}/Utilities/Release\\\\InstallIcon.bmp")
#   SET(CPACK_NSIS_INSTALLED_ICON_NAME "bin\\\\MyExecutable.exe")
#   SET(CPACK_NSIS_DISPLAY_NAME "${CPACK_PACKAGE_INSTALL_DIRECTORY} My Famous Project")
#   SET(CPACK_NSIS_HELP_LINK "http:\\\\\\\\www.my-project-home-page.org")
#   SET(CPACK_NSIS_URL_INFO_ABOUT "http:\\\\\\\\www.my-personal-home-page.com")
#   SET(CPACK_NSIS_CONTACT "me@my-personal-home-page.com")
#   SET(CPACK_NSIS_MODIFY_PATH ON)
# ELSE(WIN32 AND NOT UNIX)
#   SET(CPACK_STRIP_FILES "bin/MyExecutable")
#   SET(CPACK_SOURCE_STRIP_FILES "")
# ENDIF(WIN32 AND NOT UNIX)
# SET(CPACK_PACKAGE_EXECUTABLES "MyExecutable" "My Executable")
# INCLUDE(CPACK)
