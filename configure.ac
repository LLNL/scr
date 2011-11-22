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
AM_PROG_CC_C_O
#AC_PROG_CXX
#X_AC_PROG_MPICC
LX_FIND_MPI
X_AC_YOGRT
X_AC_GCS
X_AC_MYSQL
X_AC_MACHINE
X_AC_FILELOCK
X_AC_BYTESWAP
X_AC_MOAB

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
  scripts/common/Makefile \
  scripts/common/scr_cntl_dir \
  scripts/common/scr_check_node \
  scripts/common/scr_glob_hosts \
  scripts/common/scr_prerun \
  scripts/common/scr_postrun \
  scripts/common/scr_test_datemanip \
  scripts/common/scr_test_runtime \
  scripts/common/scr_param.pm \
  scripts/common/scr_watchdog \
  scripts/TLCC/Makefile \
  scripts/TLCC/scr_run \
  scripts/TLCC/scr_env \
  scripts/TLCC/scr_halt \
  scripts/TLCC/scr_list_down_nodes \
  scripts/TLCC/scr_scavenge \
  scripts/TLCC/scr_inspect \
  scripts/TLCC/scr_kill_jobstep \
  scripts/TLCC/scr_get_jobstep_id \
  scripts/cray_xt/Makefile \
  scripts/cray_xt/scr_run \
  scripts/cray_xt/scr_env \
  scripts/cray_xt/scr_halt \
  scripts/cray_xt/scr_list_down_nodes \
  scripts/cray_xt/scr_scavenge \
  scripts/cray_xt/scr_inspect \
  scripts/cray_xt/scr_kill_jobstep \
  scripts/cray_xt/scr_get_jobstep_id \
  examples/Makefile \
  examples/makefile.examples \
  doc/Makefile \
])
AC_OUTPUT

