#  X_AC_MOAB ([action-if-found], [action-if-not-found])
#  ------------------------------------------------------------------------
#  Adds --with-moab=<dir> to the configure help message.
#  If not found , have_moab is set to "no".  If found, exports
#  MOAB_LDFLAGS MOAB_CFLAGS MOAB_CPPFLAGS MOAB_RPATH with AC_SUBST.
#


AC_DEFUN([X_AC_MOAB], [

  # basic checks that the include and library files exist at the given prefix
  # after running this:
  # MOAB_PREFIX will equal prefix
  # MOAB_CPPFLAGS will be -I$MOAB_PREFIX/include
  # MOAB_LDFLAGS will be -L$MOAB_PREFIX/lib -lcmoab -lmoab -lmoabdb -lsqlite3 -lpthread -ldl -lm -lmcom -lminit
  # MOAB_RPATH will be  -R $MOAB_PREFIX/lib
  # have_moab will be yes or no, depending
  AC_ARG_WITH([moab],
    [AS_HELP_STRING([--with-moab=prefix],
      [Add the compile and link search paths for moab]
    )],
    [ MOAB_PREFIX="$withval"
      want_moab=yes
      LX_HEADER_SUBST(moab, [mapi.h], MOAB, [-I$MOAB_PREFIX/include])
      AC_MSG_CHECKING([for MOAB libraries])
      LX_SAFE_LINK_IFELSE([-I$MOAB_PREFIX/include],
                          [-L$MOAB_PREFIX/lib -lcmoab -lmoab -lmoabdb -lsqlite3 -lpthread -ldl -lm -lmcom -lminit],
                          [-lcmoab -lmoab -lmoabdb -lsqlite3 -lpthread -ldl -lm -lmcom -lminit],
                          [AC_LANG_PROGRAM([[#include <mapi.h>]],
                           [[long int Time; int status = MCCJobGetRemainingTime(0,0,&Time,0);]])],
                           [have_moab=yes
                            MOAB_LDFLAGS="-L$MOAB_PREFIX/lib -lcmoab -lmoab -lmoabdb -lsqlite3 -lpthread -ldl -lm -lmcom -lminit"
                            MOAB_CFLAGS=-I$MOAB_PREFIX/include
                            MOAB_CXXFLAGS=-I$MOAB_PREFIX/include
                            MOAB_RPATH=-L$MOAB_PREFIX/lib], 
                           [have_moab=no])

    ]
  )

  if [test $want_moab == yes]; then
    if [test $have_moab != no]; then
      AC_MSG_RESULT([Yes, we have MOAB libraries])
      AC_DEFINE(HAVE_MOAB, [1], [Define to 1 if you have a valid installation of MOAB])
      AC_SUBST(MOAB_CFLAGS)
      AC_SUBST(MOAB_CPPFLAGS)
      AC_SUBST(MOAB_LDFLAGS)
      AC_SUBST(MOAB_RPATH)
    else
      AC_MSG_RESULT([No, unable to find MOAB libraries])
    fi
  fi
])

