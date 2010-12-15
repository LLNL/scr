AC_DEFUN([X_AC_PROG_PDSH], [

  # check that the OS enables #! usage to run a script, e.g., #!/usr/bin/perl -w
  AC_SYS_INTERPRETER
  if test "x$interpval" = xno; then
    AC_MSG_WARN([SCR scripts dynamically select the interpreter to run a script.])
  fi

  # enable user to specify path that contains pdsh and dshbak commands
  AC_ARG_WITH([pdsh],
    AC_HELP_STRING([--with-pdsh=PATH], [Path to pdsh and dshbak commands]),
    [ case "$withval" in
        no)  ac_pdsh_path= ;;
        yes) ac_pdsh_path= ;;
        *)   ac_pdsh_path=$withval ;;
      esac
    ]
  )

  # check for pdsh command
  if test "x$ac_pdsh_path" = "x"; then
    # user did not specify, so search the standard PATH
    AC_CHECK_PROG([PDSH_EXE], [pdsh], [pdsh], [no])
    if test "x$PDSH_EXE" = xno; then
      # could not find pdsh, print a warning, and hardcode to pdsh
      AC_MSG_WARN([Unable to locate pdsh command, see README])
      PDSH_EXE=pdsh
    fi
  else
    PDSH_EXE=${ac_pdsh_path}/pdsh
  fi
  AC_SUBST(PDSH_EXE)

  # check for dshbak command
  if test "x$ac_pdsh_path" = "x"; then
    # user did not specify, so search the standard PATH
    AC_CHECK_PROG([DSHBAK_EXE], [dshbak], [dshbak], [no])
    if test "x$DSHBAK_EXE" = xno; then
      # could not find dshbak, print a warning, and hardcode to dshbak
      AC_MSG_WARN([Unable to locate dshbak command, see README])
      DSHBAK_EXE=dshbak
    fi
  else
    DSHBAK_EXE=${ac_pdsh_path}/dshbak
  fi
  AC_SUBST(DSHBAK_EXE)

  # TODO: check for Hostlist
  # TODO: check for Date::Manip
  # see http://www.nongnu.org/autoconf-archive/macros-by-category.html#PERL

  AC_PROG_PERL_MODULES([Date::Manip], [], [AC_MSG_WARN([Could not find Date::Manip perl module, see README])])

])
