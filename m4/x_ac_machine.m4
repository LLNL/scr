# set the machine name to use for platform specific code 

AC_DEFUN([X_AC_MACHINE], [

  # set the base directory for the checkpoint cache
  AC_ARG_WITH(
    [machine-name],
    AS_HELP_STRING(--with-machine-name=NAME,Specify the name of the computer),
    [MACHINE_NAME="$withval"],
    [MACHINE_NAME="TLCC"])
  AC_SUBST([MACHINE_NAME])
  AC_DEFINE_UNQUOTED([MACHINE_NAME], ["$MACHINE_NAME"], [Specifies the name of the machine])

])
