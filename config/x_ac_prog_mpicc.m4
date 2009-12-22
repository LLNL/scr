# FIXME: this is just a placeholder for real selection of mpicc options
AC_DEFUN([X_AC_PROG_MPICC], [

    case "$host" in
      *-*-aix*)
        CC=mpxlc
        #LD=mpxlc
        AC_SUBST([CC])
        #AC_SUBST([LD])
        ;;
      *)
        CC=mpicc
        #LD=mpicc
        AC_SUBST([CC])
        #AC_SUBST([LD])
        ;;
    esac

])
