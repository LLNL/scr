AC_DEFUN([X_AC_MYSQL],
[
  AC_MSG_CHECKING([mysql client library])
  AC_ARG_WITH([mysql],
    AC_HELP_STRING([--with-mysql], [Link to mysql client library]),
    [ case "$withval" in
        no)  ac_mysql_test=no ;;
        yes) ac_mysql_test=yes ;;
        *)   ac_mysql_test=yes ;;
      esac
    ],
    [ ac_mysql_test=no ]
  )
  AC_MSG_RESULT([${ac_mysql_test=yes}])

  if test "x$ac_mysql_test" = "xyes"; then
      # user wants libmysqlclient enabled, so let's define it in the source code
      AC_DEFINE([HAVE_LIBMYSQLCLIENT], [1], [Define if libmysql is available])

      LDFLAGS_SAVE="${LDFLAGS}"
      TMP_MYSQL=`mysql_config --libs`
      TMP_MYSQL_LDFLAGS=""
      TMP_MYSQL_LIBS=""

      for TMPARG in ${TMP_MYSQL}; do
          TMP_MYSQL_LDFLAGS_COUNT=`echo ${TMPARG} | grep -c "\-L"`
          TMP_MYSQL_LIBS_COUNT=`echo ${TMPARG} | grep -c "\-l"`

          if [[ ${TMP_MYSQL_LDFLAGS_COUNT} -gt 0 ]]; then
              TMP_MYSQL_LDFLAGS="${TMP_MYSQL_LDFLAGS} ${TMPARG}"
          elif [[ ${TMP_MYSQL_LIBS_COUNT} -gt 0 ]]; then
              TMP_MYSQL_LIBS="${TMP_MYSQL_LIBS} ${TMPARG}"
          fi
      done

      LDFLAGS="${TMP_MYSQL_LDFLAGS}"
      AC_CHECK_LIB([mysqlclient], [mysql_init], [x_ac_mysql_have_mysqlclient=yes], [])
      LDFLAGS="${LDFLAGS_SAVE}"
  fi

  if test "x$x_ac_mysql_have_mysqlclient" = "xyes"; then
      MYSQL_CPPFLAGS=`mysql_config --include`
      MYSQL_LDFLAGS="${TMP_MYSQL_LDFLAGS}"
      MYSQL_LIBS="${TMP_MYSQL_LIBS}"
  else
      MYSQL_CPPFLAGS=""
      MYSQL_LDFLAGS=""
      MYSQL_LIBS=""
  fi

  AC_SUBST(MYSQL_CPPFLAGS)
  AC_SUBST(MYSQL_LDFLAGS)
  AC_SUBST(MYSQL_LIBS)
])
