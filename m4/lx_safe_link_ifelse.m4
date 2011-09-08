#########################################################################
#
#  LX_SAFE_LINK_IFELSE (cflags, ldflags, libs, input, [action-if-true], [action-if-false])
#
#  This is a safe wrapper for AC_LINK_IFELSE, which will return
#  with $LIBS and $LDFLAGS in their original state
#  The input "cflags" is appended to CFLAGS before the call
#  The input "ldflags" is appended to LDFLAGS before the call
#  The input "libs" is appended to LIBS before the call
#  to AC_LINK_IFELSE and removed after

#
#########################################################################
AC_DEFUN([LX_SAFE_LINK_IFELSE],
[
    LX_SAFE_LINK_IFELSE_OLD_CFLAGS="$CFLAGS"
    LX_SAFE_LINK_IFELSE_OLD_LDFLAGS="$LDFLAGS"
    LX_SAFE_LINK_IFELSE_OLD_LIBS="$LIBS"
    CFLAGS="$CFLAGS $1"
    LDFLAGS="$LDFLAGS $2"
    LIBS="$LIBS $3"
    AC_LINK_IFELSE($4,$5,$6)
    CFLAGS=$LX_SAFE_LINK_IFELSE_OLD_CFLAGS
    LDFLAGS=$LX_SAFE_LINK_IFELSE_OLD_LDFLAGS
    LIBS=$LX_SAFE_LINK_IFELSE_OLD_LIBS
])

