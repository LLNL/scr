# set the type of file locking to use, flock or fnctl

AC_DEFUN([X_AC_FILELOCK], [

  filelock_name=""
  AC_ARG_WITH(
    [file-lock],
    AS_HELP_STRING([--with-file-lock=NAME,Specify which type of file locking to use. See documentation for valid values, default=flock]),
    [FILELOCK_NAME="$withval"],
    [FILELOCK_NAME="flock"])
  AC_MSG_RESULT([file lock type is... $FILELOCK_NAME])
  
  if test "$FILELOCK_NAME" == "flock"; then
     AC_DEFINE([USE_FLOCK],[1],[We should use flock for locking files])
  fi
  if test "$FILELOCK_NAME" == "fnctl"; then
     AC_DEFINE([USE_FNCTL],[1],[We should use fnctl for locking files])
  fi


])
