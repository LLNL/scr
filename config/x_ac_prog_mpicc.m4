AC_DEFUN([X_AC_PROG_MPICC], [ 
  AC_ARG_VAR(MPICC, [MPI C compiler wrapper])
  AC_ARG_VAR(MPICXX, [MPI C++ compiler wrapper])
  AC_CHECK_PROGS(MPICC, mpigcc mpicc mpiicc mpxlc, $CC)
  AC_CHECK_PROGS(MPICXX, mpig++ mpiicpc mpxlC, $CXX)
  AC_SUBST(MPICC)
  AC_SUBST(MPICXX)
])

