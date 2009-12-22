AC_DEFUN([X_AC_PROG_PDSH], [

  # check that the OS enables #! usage to run a script, e.g., #!/usr/bin/perl -w
  AC_SYS_INTERPRETER
  if test "x$interpval" = xno; then
    AC_MSG_ERROR([SCR scripts dynamically select the interpreter to run a script.])
  fi

  # check for pdsh command
  AC_PATH_PROG([PDSH_EXE], [pdsh], [no])
  if test "x$PDSH_EXE" = xno; then
    AC_MSG_ERROR([unable to locate pdsh command, see README])
  fi
  AC_SUBST(PDSH_EXE)

  # check for dshbak command
  AC_PATH_PROG([DSHBAK_EXE], [dshbak], [no])
  if test "x$DSHBAK_EXE" = xno; then
    AC_MSG_ERROR([unable to locate dshbak command, see README])
  fi
  AC_SUBST(DSHBAK_EXE)

  # TODO: check for Hostlist
  # TODO: check for Date::Manip
  # see http://www.nongnu.org/autoconf-archive/macros-by-category.html#PERL

  AC_PROG_PERL_MODULES([Hostlist], [], [AC_MSG_ERROR([Could not find Hostlist perl module, see README])])

  AC_PROG_PERL_MODULES([Date::Manip], [], [AC_MSG_ERROR([Could not find Date::Manip perl module])])

])
