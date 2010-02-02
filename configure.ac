##
# Prologue.
##
AC_INIT([scr])
AC_CONFIG_AUX_DIR([config])
AC_CONFIG_SRCDIR([NEWS])
AC_CANONICAL_SYSTEM
X_AC_META
X_AC_EXPAND_INSTALL_DIRS

##
# Automake support.
##
AM_INIT_AUTOMAKE([$META_NAME], [$META_VERSION]-[$META_RELEASE])
AM_CONFIG_HEADER([config/config.h])
AM_MAINTAINER_MODE

##
# Checks for programs.
##
X_AC_PROG_MPICC
X_AC_YOGRT
X_AC_MYSQL
X_AC_PROG_PDSH
AC_PROG_LIBTOOL

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
AC_CONFIG_FILES( \
  Makefile \
  src/Makefile \
  man/Makefile \
  man/scr.1 \
  man/scr_check_complete.1 \
  man/scr_halt.1 \
  man/scr_postrun.1 \
  man/scr_prerun.1 \
  man/scr_srun.1 \
  scripts/Makefile \
  scripts/scr_cntl_dir \
  scripts/scr_check_complete \
  scripts/scr_check_node \
  scripts/scr_env \
  scripts/scr_inspect \
  scripts/scr_flush \
  scripts/scr_flush_file \
  scripts/scr_glob_hosts \
  scripts/scr_halt \
  scripts/scr_list_down_nodes \
  scripts/scr_postrun \
  scripts/scr_prerun \
  scripts/scr_retries_halt \
  scripts/scr_srun \
  scripts/scr_param.pm \
  examples/Makefile \
  examples/makefile.examples \
  doc/Makefile \
)
AC_OUTPUT

