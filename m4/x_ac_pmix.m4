##*****************************************************************************
#  AUTHOR:
#    Christopher Holguin <christopher.a.holguin@intel.com>
#
#  SYNOPSIS:
#    X_AC_PMIX()
#
#  DESCRIPTION:
#    Check the usual suspects for a pmix installation,
#    setting PMIX_CFLAGS, PMIX_LDFLAGS, and PMIX_LIBS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************

AC_DEFUN([X_AC_PMIX], [

  _x_ac_pmix_dirs="/usr /opt/freeware"
  _x_ac_pmix_libs="lib lib64"

  AC_ARG_WITH(
    [pmix],
    AS_HELP_STRING(--with-pmix=PATH,Specify libpmix path - PMIx is PMI exascale library for the PMIX machine setting only),
    [_x_ac_pmix_dirs="$withval"
     with_pmix=yes],
    [_x_ac_pmix_dirs=no
     with_pmix=no]
  )

  if test "x$_x_ac_pmix_dirs" = xno ; then
    # user explicitly wants to disable libpmix support
    PMIX_CFLAGS=""
    PMIX_LDFLAGS=""
    PMIX_LIBS=""
    # TODO: would be nice to print some message here to record in the config log
    if test "$MACHINE_NAME" == "PMIX"; then
       AC_MSG_ERROR([you can't disable pmix and use the pmix machine type. --with-pmix and PMIX_MACHINE_TYPE must always be specified together])
    fi
    AM_CONDITIONAL([INTERNAL_HAVE_LIBPMIX], false)
  else
    if test "$MACHINE_NAME" != "PMIX"; then
       AC_MSG_ERROR([you can't enable pmix and use a different machine type other than PMIX_MACHINE_TYPE. --with-pmix and PMIX_MACHINE_TYPE must always be specified together])       
    fi
    # user wants libpmix enabled, so let's define it in the source code
    AC_DEFINE([HAVE_LIBPMIX], [1], [Define if libpmix is available])

    # now let's locate the install location
    found=no

    # check for libpmix in a system default location if:
    #   --with-pmix or --without-pmix is not specified
    #   --with-pmix=yes is specified
    #   --with-pmix is specified
    if test "$with_pmix" = check || \
       test "x$_x_ac_pmix_dirs" = xyes || \
       test "x$_x_ac_pmix_dirs" = "x" ; then
      AC_CHECK_LIB([pmix], [PMIx_Init])

      # if we found it, set the build flags
      if test "$ac_cv_lib_pmix_pmix_init" = yes; then
        found=yes
        PMIX_CFLAGS=""
        PMIX_LDFLAGS=""
        PMIX_LIBS="-lpmix"
      fi
    fi

    # if we have not already found it, check the pmix_dirs
    if test "$found" = no; then
      AC_CACHE_CHECK(
        [for libpmix installation],
        [x_ac_cv_pmix_dir],
        [
          for d in $_x_ac_pmix_dirs; do
            test -d "$d" || continue
            test -d "$d/include" || continue
            test -f "$d/include/pmix.h" || continue
            for bit in $_x_ac_pmix_libs; do
              test -d "$d/$bit" || continue
        
              _x_ac_pmix_libs_save="$LIBS"
              LIBS="-L$d/$bit -lpmix $LIBS $MPI_CLDFLAGS"
              AC_LINK_IFELSE(
                AC_LANG_PROGRAM([#include <pmix.h>], [PMIx_Init(NULL, NULL, 0)]),
                AS_VAR_SET([x_ac_cv_pmix_dir], [$d]))
              LIBS="$_x_ac_pmix_libs_save"
              test -n "$x_ac_cv_pmix_dir" && break
            done
            test -n "$x_ac_cv_pmix_dir" && break
          done
      ])

      # if we found it, set the build flags
      if test -n "$x_ac_cv_pmix_dir"; then
        found=yes
        PMIX_CFLAGS="-I$x_ac_cv_pmix_dir/include"
	PMIX_CXXFLAGS="-I$x_ac_cv_pmix_dir/include"
        PMIX_LDFLAGS="-L$x_ac_cv_pmix_dir/$bit"
        PMIX_LIBS="-lpmix"
      fi
    fi

    # if we failed to find libpmix, throw an error
    if test "$found" = no ; then
      AC_MSG_ERROR([unable to locate libpmix installation])
    fi
   AM_CONDITIONAL([INTERNAL_HAVE_LIBPMIX], true)
  fi

  # propogate the build flags to our makefiles
  AC_SUBST(PMIX_CFLAGS)
  AC_SUBST(PMIX_CXXFLAGS)
  AC_SUBST(PMIX_LDFLAGS)
  AC_SUBST(PMIX_LIBS)
])
