##
# Prologue.
##
AC_INIT
AC_CONFIG_SRCDIR([src/scr.c])
AC_CONFIG_AUX_DIR(config)
AC_CONFIG_MACRO_DIR([m4])

AM_CONFIG_HEADER([config/config.h])

#AC_CANONICAL_SYSTEM

X_AC_META
X_AC_EXPAND_INSTALL_DIRS

##
# Automake support.
##
AM_INIT_AUTOMAKE([$META_NAME], [$META_VERSION]-[$META_RELEASE])

#AM_MAINTAINER_MODE

##
# Checks for programs.
##
AC_PROG_CC
#AC_PROG_CXX
#X_AC_PROG_MPICC
LX_FIND_MPI
X_AC_YOGRT
X_AC_GCS
X_AC_MYSQL

##
# enable libtool
##
AC_PROG_LIBTOOL

##
# Extra packages
##
#X_AC_MRNET
#X_AC_LAUNCHMON
X_AC_PROG_PDSH

##
# Checks for header files.
##
AC_HEADER_STDC

##
# Checks for typedefs, structures, and compiler characteristics.
##
AC_C_BIGENDIAN

##
# Checks for library functions.
##

##
# Config the SCR package
##
X_AC_CONFIG_SCR

##
# Epilogue.
##
AC_CONFIG_FILES([ \
  Makefile \
  src/Makefile \
  man/Makefile \
  man/scr.1 \
  man/scr_index.1 \
  man/scr_halt.1 \
  man/scr_postrun.1 \
  man/scr_prerun.1 \
  man/scr_srun.1 \
  scripts/Makefile \
  scripts/scr_cntl_dir \
  scripts/scr_check_node \
  scripts/scr_env \
  scripts/scr_inspect \
  scripts/scr_scavenge \
  scripts/scr_glob_hosts \
  scripts/scr_halt \
  scripts/scr_list_down_nodes \
  scripts/scr_postrun \
  scripts/scr_prerun \
  scripts/scr_srun \
  scripts/scr_test_datemanip \
  scripts/scr_test_runtime \
  scripts/scr_param.pm \
  examples/Makefile \
  examples/makefile.examples \
  doc/Makefile \
])
AC_OUTPUT

