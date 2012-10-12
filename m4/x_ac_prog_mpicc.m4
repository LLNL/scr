AC_DEFUN([X_AC_PROG_MPICC], [ 
  AC_ARG_VAR(MPICC, [MPI C compiler wrapper])
  AC_CHECK_PROGS(MPICC, mpicc mpigcc mpiicc mpxlc, mpixlc, mpicc)
  AC_SUBST(MPICC)
])

