# set default parameters for SCR configuration

AC_DEFUN([X_AC_CONFIG_SCR], [

  # set the base directory for the checkpoint cache
  AC_ARG_WITH(
    [scr-cache-base],
    AS_HELP_STRING(--with-scr-cache-base=PATH,Specify base path for SCR cache directory),
    [SCR_CACHE_BASE="$withval"],
    [SCR_CACHE_BASE="/tmp"])
  AC_SUBST([SCR_CACHE_BASE])
  AC_DEFINE_UNQUOTED([SCR_CACHE_BASE], ["$SCR_CACHE_BASE"], [Specifies default base path for SCR cache directory])

  # set the base directory for the control directory
  AC_ARG_WITH(
    [scr-cntl-base],
    AS_HELP_STRING(--with-scr-cntl-base=PATH,Specify base path for SCR control directory),
    [SCR_CNTL_BASE="$withval"],
    [SCR_CNTL_BASE="/tmp"])
  AC_SUBST([SCR_CNTL_BASE])
  AC_DEFINE_UNQUOTED([SCR_CNTL_BASE], ["$SCR_CNTL_BASE"], [Specifies default base path for SCR control directory])

  # set the path to the configuration file
  AC_ARG_WITH(
    [scr-config-file],
    AS_HELP_STRING(--with-scr-config-file=PATH,Specify full path and file name for SCR config file),
    [SCR_CONFIG_FILE="$withval"],
    [SCR_CONFIG_FILE="/etc/scr/scr.conf"])
  AC_SUBST([SCR_CONFIG_FILE])
  AC_DEFINE_UNQUOTED([SCR_CONFIG_FILE], ["$SCR_CONFIG_FILE"], [Specifies full path and file name to SCR config file])

])
