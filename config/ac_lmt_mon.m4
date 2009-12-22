AC_DEFUN([AC_LMT_MON],
[
  AC_MSG_CHECKING([for whether to build lmt monitor modules])
  AC_ARG_WITH([lmt],
    AC_HELP_STRING([--with-lmt-mon], [Build lmt monitor modules]),
    [ case "$withval" in
        no)  ac_lmt_mon_test=no ;;
        yes) ac_lmt_mon_test=yes ;;
        *)   ac_lmt_mon_test=yes ;;
      esac
    ]
  )
  AC_MSG_RESULT([${ac_lmt_mon_test=yes}])

  if test "$ac_lmt_mon_test" = "yes"; then
      LDFLAGS_SAVE="${LDFLAGS}"
      TMP_LMTMON=`mysql_config --libs`
      TMP_LMT_MON_LDFLAGS=""
      TMP_LMT_MON_LIBS=""

      for TMPARG in ${TMP_LMTMON}; do
          TMP_LMT_MON_LDFLAGS_COUNT=`echo ${TMPARG} | grep -c "\-L"`
          TMP_LMT_MON_LIBS_COUNT=`echo ${TMPARG} | grep -c "\-l"`

          if [[ ${TMP_LMT_MON_LDFLAGS_COUNT} -gt 0 ]]; then
              TMP_LMT_MON_LDFLAGS="${TMP_LMT_MON_LDFLAGS} ${TMPARG}"
          elif [[ ${TMP_LMT_MON_LIBS_COUNT} -gt 0 ]]; then
              TMP_LMT_MON_LIBS="${TMP_LMT_MON_LIBS} ${TMPARG}"
          fi
      done

      LDFLAGS="${TMP_LMT_MON_LDFLAGS}"
      AC_CHECK_LIB([mysqlclient], [mysql_init], [ac_lmt_mon_have_mysqlclient=yes], [])
      LDFLAGS="${LDFLAGS_SAVE}"
  fi

  if test "$ac_lmt_mon_have_mysqlclient" = "yes"; then
      LMT_MON_LDFLAGS="${TMP_LMT_MON_LDFLAGS}"
      LMT_MON_LIBS="${TMP_LMT_MON_LIBS}"
      MANPAGE_LMT_MON=0
      ac_with_lmt_mon=yes
  else
      MANPAGE_LMT_MON=0
      ac_with_lmt_mon=no
  fi

  AC_SUBST(LMT_MON_LDFLAGS)
  AC_SUBST(LMT_MON_LIBS)
  AC_SUBST(MANPAGE_LMT_MON)
])
