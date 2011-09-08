##########################################################################
# Written by Todd Gamblin, tgamblin@llnl.gov.
#
#  LX_HEADER_SUBST (name, header, NAME, includes)
#   ------------------------------------------------------------------------
#  This tests for the presence of a header file, given its name, and a directory
#  to search. If found, it uses AC_SUBST to export NAME_CPPFLAGS for the header.
#  Standard var CPPFLAGS is unmodified.  If the header is not found,
#  then have_name is set to "no".
#
##########################################################################
AC_DEFUN([LX_HEADER_SUBST],
[
  $3_CPPFLAGS="$4"
  LX_HEADER_SUBST_OLD_CPPFLAGS="$CPPFLAGS"
  CPPFLAGS="$$3_CPPFLAGS $CPPFLAGS"

  AC_CHECK_HEADER([$2],
                  [have_$1=yes],
                  [AC_MSG_NOTICE([Couldn't find $2.])
                   have_$1=no])
  AC_SUBST($3_CPPFLAGS)
  CPPFLAGS="$LX_HEADER_SUBST_OLD_CPPFLAGS"
])

