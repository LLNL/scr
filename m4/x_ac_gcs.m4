##*****************************************************************************
#  AUTHOR:
#    Adam Moody <moody20@llnl.gov>
#
#  SYNOPSIS:
#    X_AC_GCS()
#
#  DESCRIPTION:
#    Check the usual suspects for a GCS installation,
#    setting GCS_CFLAGS, GCS_LDFLAGS, and GCS_LIBS as necessary.
#
#  WARNINGS:
#    This macro must be placed after AC_PROG_CC and before AC_PROG_LIBTOOL.
##*****************************************************************************

AC_DEFUN([X_AC_GCS], [

  _x_ac_gcs_dirs="/usr /opt/freeware"
  _x_ac_gcs_libs="lib64 lib"

  AC_ARG_WITH(
    [gcs],
    AS_HELP_STRING(--with-gcs=PATH,Specify libgcs path),
    [_x_ac_gcs_dirs="$withval"
     with_gcs=yes],
    [_x_ac_gcs_dirs=no
     with_gcs=no]
  )

  if test "x$_x_ac_gcs_dirs" = xno ; then
    # user explicitly wants to disable libgcs support
    GCS_CFLAGS=""
    GCS_LDFLAGS=""
    GCS_LIBS=""
    # TODO: would be nice to print some message here to record in the config log
  else
    # user wants libgcs enabled, so let's define it in the source code
    AC_DEFINE([HAVE_LIBGCS], [1], [Define if libgcs is available])

    # now let's locate the install location
    found=no

    # check for libgcs in a system default location if:
    #   --with-gcs or --without-gcs is not specified
    #   --with-gcs=yes is specified
    #   --with-gcs is specified
    if test "$with_gcs" = check || \
       test "x$_x_ac_gcs_dirs" = xyes || \
       test "x$_x_ac_gcs_dirs" = "x" ; then
      AC_CHECK_LIB([gcs], [GCS_Comm_split])

      # if we found it, set the build flags
      if test "$ac_cv_lib_gcs_GCS_Comm_split" = yes; then
        found=yes
        GCS_CFLAGS=""
        GCS_LDFLAGS=""
        GCS_LIBS="-lgcs"
      fi
    fi

    # if we have not already found it, check the gcs_dirs
    if test "$found" = no; then
      AC_CACHE_CHECK(
        [for libgcs installation],
        [x_ac_cv_gcs_dir],
        [
          for d in $_x_ac_gcs_dirs; do
            test -d "$d" || continue
            test -d "$d/include" || continue
            test -f "$d/include/gcs.h" || continue
            for bit in $_x_ac_gcs_libs; do
              test -d "$d/$bit" || continue
        
              _x_ac_gcs_libs_save="$LIBS"
              LIBS="-L$d/$bit -lgcs $LIBS $MPI_CLDFLAGS"
              AC_LINK_IFELSE(
                AC_LANG_CALL([], [GCS_Comm_split]),
                AS_VAR_SET([x_ac_cv_gcs_dir], [$d]))
              LIBS="$_x_ac_gcs_libs_save"
              test -n "$x_ac_cv_gcs_dir" && break
            done
            test -n "$x_ac_cv_gcs_dir" && break
          done
      ])

      # if we found it, set the build flags
      if test -n "$x_ac_cv_gcs_dir"; then
        found=yes
        GCS_CFLAGS="-I$x_ac_cv_gcs_dir/include"
        GCS_LDFLAGS="-L$x_ac_cv_gcs_dir/$bit"
        GCS_LIBS="-lgcs"
      fi
    fi

    # if we failed to find libgcs, throw an error
    if test "$found" = no ; then
      AC_MSG_ERROR([unable to locate libgcs installation])
    fi
  fi

  # propogate the build flags to our makefiles
  AC_SUBST(GCS_CFLAGS)
  AC_SUBST(GCS_LDFLAGS)
  AC_SUBST(GCS_LIBS)
])
