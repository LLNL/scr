##
# Prologue.
##
AC_INIT([SCR],[1.1.8],[scalablecr-discuss@lists.sourceforge.net],[scr],[http://sourceforge.net/projects/scalablecr])
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
AM_INIT_AUTOMAKE([foreign])

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
X_AC_DTCMP
X_AC_MYSQL
X_AC_MACHINE
X_AC_FILELOCK
X_AC_BYTESWAP
X_AC_MOAB

# TODO create an m4
AM_CONDITIONAL([INTERNAL_HAVE_LIBDATAWARP], [false])

##
# Enable file swapping based on machine name
##
case $MACHINE_NAME in
  *TLCC*) FILE_EXT="_tlcc" ;;
  *cray_xt*) FILE_EXT="_cray_xt" ;;
  *) FILE_EXT='' ;;
esac
AC_SUBST([FILE_EXT])



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
X_AC_CPPR
X_AC_PMIX

##
# Checks for header files.
##
AC_HEADER_STDC

##
# Include MKDIR_P
##
AC_PROG_MKDIR_P

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
#AC_CONFIG_FILES([ \
#  Makefile \
#  src/Makefile \
#  man/Makefile \
#  man/scr.1 \
#  man/scr_index.1 \
#  man/scr_halt.1 \
#  man/scr_postrun.1 \
#  man/scr_prerun.1 \
#  man/scr_srun.1 \
#  scripts/Makefile \
#  scripts/common/Makefile \
#  scripts/common/scr_cntl_dir \
#  scripts/common/scr_check_node \
#  scripts/common/scr_glob_hosts \
#  scripts/common/scr_prerun \
#  scripts/common/scr_postrun \
#  scripts/common/scr_test_datemanip \
#  scripts/common/scr_test_runtime \
#  scripts/common/scr_param.pm \
#  scripts/common/scr_watchdog \
#  scripts/TLCC/Makefile \
#  scripts/TLCC/scr_run \
#  scripts/TLCC/scr_env \
#  scripts/TLCC/scr_halt \
#  scripts/TLCC/scr_list_down_nodes \
#  scripts/TLCC/scr_scavenge \
#  scripts/TLCC/scr_inspect \
#  scripts/TLCC/scr_kill_jobstep \
#  scripts/TLCC/scr_get_jobstep_id \
#  scripts/cray_xt/Makefile \
#  scripts/cray_xt/scr_run \
#  scripts/cray_xt/scr_env \
#  scripts/cray_xt/scr_halt \
#  scripts/cray_xt/scr_list_down_nodes \
#  scripts/cray_xt/scr_scavenge \
#  scripts/cray_xt/scr_inspect \
#  scripts/cray_xt/scr_kill_jobstep \
#  scripts/cray_xt/scr_get_jobstep_id \
#  examples/Makefile \
#  examples/makefile.examples \
#  doc/Makefile \
#])
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
  scripts/common/scr_prefix \
  scripts/common/scr_list_dir \
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
  scripts/PMIX/Makefile \
  scripts/PMIX/scr_run \
  scripts/PMIX/scr_env \
  scripts/PMIX/scr_halt \
  scripts/PMIX/scr_list_down_nodes \
  scripts/PMIX/scr_scavenge \
  scripts/PMIX/scr_inspect \
  scripts/PMIX/scr_kill_jobstep \
  scripts/PMIX/scr_get_jobstep_id \
  scripts/LSF/Makefile \
  scripts/LSF/scr_run \
  scripts/LSF/scr_env \
  scripts/LSF/scr_halt \
  scripts/LSF/scr_list_down_nodes \
  scripts/LSF/scr_scavenge \
  scripts/LSF/scr_inspect \
  scripts/LSF/scr_kill_jobstep \
  scripts/LSF/scr_get_jobstep_id \
  examples/Makefile \
  examples/makefile.examples \
  doc/Makefile \
])
AC_OUTPUT

