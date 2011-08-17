AC_DEFUN([X_AC_BYTESWAP],
[
  AC_MSG_CHECKING([looking for byteswap.h])
  AC_CHECK_HEADER([byteswap.h], [AC_DEFINE([HAVE_BYTESWAP_H],[1],[If we have byteswap.h])], [AC_MSG_RESULT([byteswap.h not found, using scr_byteswap.h])])
])
