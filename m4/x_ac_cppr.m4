##*****************************************************************************
#  AUTHOR:
#    Christopher Holguin <christopher.a.holguin@intel.com>
#
#  SYNOPSIS:
#    X_AC_CPPR()
#
#  DESCRIPTION:
#    Check the usual suspects for a cppr installation,
#    setting CPPR_CFLAGS, CPPR_LDFLAGS, and CPPR_LIBS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************

AC_DEFUN([X_AC_CPPR], [

  _x_ac_cppr_dirs="/usr /opt/freeware"
  _x_ac_cppr_libs="lib lib64"

  AC_ARG_WITH(
    [cppr],
    AS_HELP_STRING(--with-cppr=PATH,Specify libcppr path - CPPR is Common Persistent Memory POSIX Runtime the IO library for Aurora),
    [_x_ac_cppr_dirs="$withval"
     with_cppr=yes],
    [_x_ac_cppr_dirs=no
     with_cppr=no]
  )

  if test "x$_x_ac_cppr_dirs" = xno ; then
    # user explicitly wants to disable libcppr support
    CPPR_CFLAGS=""
    CPPR_LDFLAGS=""
    CPPR_LIBS=""
    # TODO: would be nice to print some message here to record in the config log
  else
    # user wants libcppr enabled, so let's define it in the source code
    AC_DEFINE([HAVE_LIBCPPR], [1], [Define if libcppr is available])

    # now let's locate the install location
    found=no

    # check for libcppr in a system default location if:
    #   --with-cppr or --without-cppr is not specified
    #   --with-cppr=yes is specified
    #   --with-cppr is specified
    if test "$with_cppr" = check || \
       test "x$_x_ac_cppr_dirs" = xyes || \
       test "x$_x_ac_cppr_dirs" = "x" ; then
      AC_CHECK_LIB([cppr], [cppr_status])

      # if we found it, set the build flags
      if test "$ac_cv_lib_cppr_cppr_init" = yes; then
        found=yes
        CPPR_CFLAGS=""
        CPPR_LDFLAGS=""
        CPPR_LIBS="-lcppr"
      fi
    fi

    # if we have not already found it, check the cppr_dirs
    if test "$found" = no; then
      AC_CACHE_CHECK(
        [for libcppr installation],
        [x_ac_cv_cppr_dir],
        [
          for d in $_x_ac_cppr_dirs; do
            test -d "$d" || continue
            test -d "$d/include" || continue
            test -f "$d/include/cppr.h" || continue
            for bit in $_x_ac_cppr_libs; do
              test -d "$d/$bit" || continue
        
              _x_ac_cppr_libs_save="$LIBS"
              LIBS="-L$d/$bit $LDFLAGS -lcppr $LIBS $MPI_CLDFLAGS"
              AC_LINK_IFELSE(
                AC_LANG_CALL([], [cppr_status]),
                AS_VAR_SET([x_ac_cv_cppr_dir], [$d]))
              LIBS="$_x_ac_cppr_libs_save"
              test -n "$x_ac_cv_cppr_dir" && break
            done
            test -n "$x_ac_cv_cppr_dir" && break
          done
      ])

      # if we found it, set the build flags
      if test -n "$x_ac_cv_cppr_dir"; then
        found=yes
        CPPR_CFLAGS="-I$x_ac_cv_cppr_dir/include"
        CPPR_LDFLAGS="-L$x_ac_cv_cppr_dir/$bit"
        CPPR_LIBS="-lcppr "
      fi
    fi

    # if we failed to find libcppr, throw an error
    if test "$found" = no ; then
      AC_MSG_ERROR([unable to locate libcppr installation])
    fi
    AC_SUBST(HAVE_LIBCPPR, "1")
    
  fi

  # propogate the build flags to our makefiles
  AC_SUBST(CPPR_CFLAGS)
  AC_SUBST(CPPR_LDFLAGS)
  AC_SUBST(CPPR_LIBS)
])
