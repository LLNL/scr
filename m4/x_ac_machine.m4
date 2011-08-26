# set the machine name to use for platform specific code 

AC_DEFUN([X_AC_MACHINE], [

  # set the base directory for the checkpoint cache
  machine_name=""
  AC_ARG_WITH(
    [machine-name],
    AS_HELP_STRING([--with-machine-name=NAME,Specify the type of the machine. See documentation for valid values, default=TLCC]),
    [MACHINE_NAME="$withval"],
    [MACHINE_NAME="TLCC"])
  AC_MSG_RESULT([machine type is... $MACHINE_NAME])
  AC_SUBST([MACHINE_NAME])
  scr_tlcc="0"
  scr_cray_xt="1"
  scr_machine_int="10"
  if test "$MACHINE_NAME" == "TLCC"; then
   scr_machine_int=$scr_tlcc
  fi
  if test "$MACHINE_NAME" == "cray_xt"; then
   scr_machine_int=$scr_cray_xt
  fi
  AC_DEFINE_UNQUOTED([SCR_TLCC],[$scr_tlcc], [Define for TLCC machine type.])
  AC_DEFINE_UNQUOTED([SCR_CRAY_XT],[$scr_cray_xt], [Define for CRAY_XT machine type.])
  AC_DEFINE_UNQUOTED([SCR_MACHINE_NAME],[$MACHINE_NAME],[Specify the type of machine.])
  AC_DEFINE_UNQUOTED([SCR_MACHINE_TYPE],[$scr_machine_int],[Specify the type of machine.])

])
