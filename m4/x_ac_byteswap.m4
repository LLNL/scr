AC_DEFUN([X_AC_BYTESWAP],
[
  AC_MSG_CHECKING([which byteswap.h to use])
  AC_ARG_WITH([scr_byteswap],
     AC_HELP_STRING([--with-scr-byteswap],[Use SCR's byteswap.h, default=no]),
     [ case "$withval" in
          no) ac_scr_byteswap=no ;;
          yes) ac_scr_byteswap=yes ;;
          *) ac_scr_byteswap=yes;;
       esac
     ],
     [ ac_scr_byteswap=no]
     )
  if test "x$ac_scr_byteswap" = "xno"; then
  AC_CHECK_HEADER([byteswap.h], [AC_DEFINE([HAVE_BYTESWAP_H],[1],[If we have byteswap.h])], [AC_MSG_RESULT([byteswap.h not found, using scr_byteswap.h])])
  fi

])

