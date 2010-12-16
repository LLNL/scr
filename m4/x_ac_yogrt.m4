##*****************************************************************************
#  AUTHOR:
#    Christopher Morrone <morrone2@llnl.gov>
#
#  SYNOPSIS:
#    X_AC_YOGRT()
#
#  DESCRIPTION:
#    Check the usual suspects for a libyogrt installation,
#    updating CPPFLAGS and LDFLAGS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************

AC_DEFUN([X_AC_YOGRT], [

  _x_ac_yogrt_dirs="/usr /opt/freeware"
  _x_ac_yogrt_libs="lib64 lib"

  AC_ARG_WITH(
    [yogrt],
    AS_HELP_STRING(--with-yogrt=PATH,Enable libyogrt and optionally specify path),
    [_x_ac_yogrt_dirs="$withval"
     with_yogrt=yes],
    [_x_ac_yogrt_dirs=no
     with_yogrt=no]
  )

  if test "x$_x_ac_yogrt_dirs" = xno ; then
    # user explicitly wants to disable libyogrt support
    YOGRT_CPPFLAGS=""
    YOGRT_LDFLAGS=""
    YOGRT_LIBS=""
    # TODO: would be nice to print some message here to record in the config log
  else
    # user wants libyogrt enabled, so let's define it in the source code
    AC_DEFINE([HAVE_LIBYOGRT], [1], [Define if libyogrt is available])

    # now let's locate the install location
    found=no

    # check for libyogrt in a system default location if:
    #   --with-yogrt or --without-yogrt is not specified
    #   --with-yogrt=yes is specified
    #   --with-yogrt is specified
    if test "$with_yogrt" = check || \
       test "x$_x_ac_yogrt_dirs" = xyes || \
       test "x$_x_ac_yogrt_dirs" = "x" ; then
      AC_CHECK_LIB([yogrt], [yogrt_get_time])

      # if we found it, set the build flags
      if test "$ac_cv_lib_yogrt_yogrt_get_time" = yes; then
        found=yes
        YOGRT_CPPFLAGS=""
        YOGRT_LDFLAGS=""
        YOGRT_LIBS="-lyogrt"
      fi
    fi

    # if we have not already found it, check the yogrt_dirs
    if test "$found" = no; then
      AC_CACHE_CHECK(
        [for libyogrt installation],
        [x_ac_cv_yogrt_dir],
        [
          for d in $_x_ac_yogrt_dirs; do
            test -d "$d" || continue
            test -d "$d/include" || continue
            test -f "$d/include/yogrt.h" || continue
            for bit in $_x_ac_yogrt_libs; do
              test -d "$d/$bit" || continue
        
              _x_ac_yogrt_libs_save="$LIBS"
              LIBS="-L$d/$bit -lyogrt $LIBS"
              AC_LINK_IFELSE(
                AC_LANG_CALL([], [yogrt_get_time]),
                AS_VAR_SET([x_ac_cv_yogrt_dir], [$d]))
              LIBS="$_x_ac_yogrt_libs_save"
              test -n "$x_ac_cv_yogrt_dir" && break
            done
            test -n "$x_ac_cv_yogrt_dir" && break
          done
      ])

      # if we found it, set the build flags
      if test -n "$x_ac_cv_yogrt_dir"; then
        found=yes
        YOGRT_CPPFLAGS="-I$x_ac_cv_yogrt_dir/include"
        YOGRT_LDFLAGS="-L$x_ac_cv_yogrt_dir/$bit"
        YOGRT_LIBS="-lyogrt"
      fi
    fi

    # if we failed to find libyogrt, throw an error
    if test "$found" = no ; then
      AC_MSG_ERROR([unable to locate libyogrt installation])
    fi
  fi

  # propogate the build flags to our makefiles
  AC_SUBST(YOGRT_CPPFLAGS)
  AC_SUBST(YOGRT_LDFLAGS)
  AC_SUBST(YOGRT_LIBS)
])
