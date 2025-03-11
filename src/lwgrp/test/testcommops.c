#include <stdio.h>
#include <stdlib.h>

#include "mpi.h"
#include "lwgrp.h"

int main (int argc, char* argv[])
{
  int color, key;
  double start, end;
  MPI_Comm newcomm;

  MPI_Init(&argc, &argv);

  lwgrp_comm comm;
  lwgrp_comm_build_from_mpicomm(MPI_COMM_WORLD, &comm);

  int rank, ranks;
  lwgrp_comm_rank(&comm, &rank);
  lwgrp_comm_size(&comm, &ranks);

  int* members = (int*) malloc(ranks * sizeof(int));
  int* inbuf   = (int*) malloc(ranks * sizeof(int));
  int* outbuf  = (int*) malloc(ranks * sizeof(int));
  int i;
  for (i = 0; i < ranks; i++) {
    inbuf[i]  = rank * ranks + i;
    outbuf[i] = -1;
  }

  lwgrp_comm_barrier(&comm);

  int bcastbuf = rank;
  lwgrp_comm_bcast(&bcastbuf, 1, MPI_INT, ranks-1, &comm);

  lwgrp_comm_allgather(&rank, members, 1, MPI_INT, &comm);

  lwgrp_comm_alltoall(inbuf, outbuf, 1, MPI_INT, &comm);

  int sum = -1;
#if MPI_VERSION >= 2 && MPI_SUBVERSION >= 2
  sum = -1;
  lwgrp_comm_allreduce(&rank, &sum, 1, MPI_INT, MPI_SUM, &comm);
  sum = -1;
  lwgrp_comm_scan(&rank, &sum, 1, MPI_INT, MPI_SUM, &comm);
  sum = -1;
  lwgrp_comm_exscan(&rank, &sum, 1, MPI_INT, MPI_SUM, &comm);
#endif

#if 0
  #if MPI_VERSION >= 2 && MPI_SUBVERSION >= 2
  int myval = rank*2 + 1;
  int allval;
  lwgrp_chain_allreduce(&myval, &allval, 1, MPI_INT, MPI_SUM, &group);
  printf("rank=%d input=%d output=%d\n", rank, myval, allval);
  fflush(stdout);

  int ltr_send =  1;
  int rtl_send = -1;
  int ltr_recv = 100;
  int rtl_recv = 100;
  lwgrp_chain_double_exscan(&ltr_send, &ltr_recv, &rtl_send, &rtl_recv, 1, MPI_INT, MPI_SUM, &group);
  printf("rank=%d ltr_send=%d ltr_recv=%d rtl_send=%d rtl_recv=%d\n",
    rank, ltr_send, ltr_recv, rtl_send, rtl_recv
  );
  fflush(stdout);
  #endif
#endif

  lwgrp_comm_free(&comm);

  MPI_Finalize();

  return 0;
}
