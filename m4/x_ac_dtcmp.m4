##*****************************************************************************
#  AUTHOR:
#    Adam Moody <moody20@llnl.gov>
#
#  SYNOPSIS:
#    X_AC_DTCMP()
#
#  DESCRIPTION:
#    Check the usual suspects for a DTCMP installation,
#    setting DTCMP_CFLAGS, DTCMP_LDFLAGS, and DTCMP_LIBS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************

AC_DEFUN([X_AC_DTCMP], [

  _x_ac_dtcmp_dirs="/usr /opt/freeware"
  _x_ac_dtcmp_libs="lib64 lib"

  AC_ARG_WITH(
    [dtcmp],
    AS_HELP_STRING(--with-dtcmp=PATH,Specify libdtcmp path),
    [_x_ac_dtcmp_dirs="$withval"
     with_dtcmp=yes],
    [_x_ac_dtcmp_dirs=no
     with_dtcmp=no]
  )

  if test "x$_x_ac_dtcmp_dirs" = xno ; then
    # user explicitly wants to disable libdtcmp support
    DTCMP_CFLAGS=""
    DTCMP_LDFLAGS=""
    DTCMP_LIBS=""
    # TODO: would be nice to print some message here to record in the config log
  else
    # user wants libdtcmp enabled, so let's define it in the source code
    AC_DEFINE([HAVE_LIBDTCMP], [1], [Define if libdtcmp is available])

    # now let's locate the install location
    found=no

    # check for libdtcmp in a system default location if:
    #   --with-dtcmp or --without-dtcmp is not specified
    #   --with-dtcmp=yes is specified
    #   --with-dtcmp is specified
    if test "$with_dtcmp" = check || \
       test "x$_x_ac_dtcmp_dirs" = xyes || \
       test "x$_x_ac_dtcmp_dirs" = "x" ; then
      AC_CHECK_LIB([dtcmp], [DTCMP_Rank_strings])

      # if we found it, set the build flags
      if test "$ac_cv_lib_dtcmp_DTCMP_Rank_strings" = yes; then
        found=yes
        DTCMP_CFLAGS=""
        DTCMP_LDFLAGS=""
        DTCMP_LIBS="-ldtcmp"
      fi
    fi

    # if we have not already found it, check the dtcmp_dirs
    if test "$found" = no; then
      AC_CACHE_CHECK(
        [for libdtcmp installation],
        [x_ac_cv_dtcmp_dir],
        [
          for d in $_x_ac_dtcmp_dirs; do
            test -d "$d" || continue
            test -d "$d/include" || continue
            test -f "$d/include/dtcmp.h" || continue
            for bit in $_x_ac_dtcmp_libs; do
              test -d "$d/$bit" || continue
        
              _x_ac_dtcmp_libs_save="$LIBS"
              LIBS="-L$d/$bit -ldtcmp $LIBS $MPI_CLDFLAGS"
              AC_LINK_IFELSE(
                AC_LANG_CALL([], [DTCMP_Rank_strings]),
                AS_VAR_SET([x_ac_cv_dtcmp_dir], [$d]))
              LIBS="$_x_ac_dtcmp_libs_save"
              test -n "$x_ac_cv_dtcmp_dir" && break
            done
            test -n "$x_ac_cv_dtcmp_dir" && break
          done
      ])

      # if we found it, set the build flags
      if test -n "$x_ac_cv_dtcmp_dir"; then
        found=yes
        DTCMP_CFLAGS="-I$x_ac_cv_dtcmp_dir/include"
        DTCMP_LDFLAGS="-L$x_ac_cv_dtcmp_dir/$bit"
        DTCMP_LIBS="-ldtcmp"
      fi
    fi

    # if we failed to find libdtcmp, throw an error
    if test "$found" = no ; then
      AC_MSG_ERROR([unable to locate libdtcmp installation])
    fi
  fi

  # propogate the build flags to our makefiles
  AC_SUBST(DTCMP_CFLAGS)
  AC_SUBST(DTCMP_LDFLAGS)
  AC_SUBST(DTCMP_LIBS)
])
