#include <stdio.h>
#include <stdlib.h>

#include "mpi.h"
#include "lwgrp.h"

int print_members(int testid, int rank, int size, int members[])
{
  int i;

  /* only have the root of each group print the members */
  if (size > 0 && members[0] == rank) {
    printf("Test %d -- %d: ", testid, rank);
    for (i=0; i < size; i++) {
      printf("%d, ", members[i]);
    }
    printf("\n");
    fflush(stdout);
  }

  return 0;
}

int print_comm(int testid, int rank, MPI_Comm comm)
{
  int i;

  if (comm != MPI_COMM_NULL) {
    int ranks;
    MPI_Comm_size(comm, &ranks);

    int* members_comm  = (int*) malloc(ranks * sizeof(int));
    int* members_world = (int*) malloc(ranks * sizeof(int));

    for (i = 0; i < ranks; i++) {
      members_comm[i] = i;
    }

    MPI_Group group_world, group_comm;
    MPI_Comm_group(MPI_COMM_WORLD, &group_world);
    MPI_Comm_group(comm, &group_comm);
    MPI_Group_translate_ranks(group_comm, ranks, members_comm, group_world, members_world);
    MPI_Group_free(&group_comm);
    MPI_Group_free(&group_world);

    print_members(testid, rank, ranks, members_world);

    free(members_world);
    free(members_comm);
  }

  return 0;
}

int main (int argc, char* argv[])
{
  int color, key;
  double start, end;
  MPI_Comm newcomm;

  MPI_Init(&argc, &argv);

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int size;
  int* members = (int*) malloc(ranks * sizeof(int));

  lwgrp_ring group;
  lwgrp_ring_build_from_mpicomm(MPI_COMM_WORLD, &group);

  lwgrp_ring group_split;
  lwgrp_ring_split_bin_scan(2, (rank%2), &group, &group_split);

  lwgrp_logring logring;
  lwgrp_logring_build_from_ring(&group, &logring);

  int* inbuf  = (int*) malloc(ranks * sizeof(int));
  int* outbuf = (int*) malloc(ranks * sizeof(int));
  int i;
  for (i = 0; i < ranks; i++) {
    inbuf[i]  = rank * ranks + i;
    outbuf[i] = -1;
  }

  lwgrp_logring_barrier_dissemination(&group, &logring);

  int bcastbuf = rank;
  lwgrp_logring_bcast_binomial(&bcastbuf, 1, MPI_INT, 0, &group, &logring);

  lwgrp_logring_allgather_brucks(&rank, members, 1, MPI_INT, &group, &logring);

  lwgrp_logring_alltoall_brucks(inbuf, outbuf, 1, MPI_INT, &group, &logring);

  int sum = -1;
  lwgrp_logring_allreduce_recursive(&rank, &sum, 1, MPI_INT, MPI_SUM, &group, &logring);
  lwgrp_logring_scan_recursive(&rank, &sum, 1, MPI_INT, MPI_SUM, &group, &logring);
  lwgrp_logring_exscan_recursive(&rank, &sum, 1, MPI_INT, MPI_SUM, &group, &logring);

#if 0
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

  lwgrp_ring_free(&group);
  MPI_Finalize();
  return 0;

  int tag1 = 0;
  int tag2 = 1;

  color = 0;
  key = rank;
  start = MPI_Wtime();
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, tag1, tag2, &size, members);
  end = MPI_Wtime();
  print_members(1, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  lwgrp_comm_split_create(MPI_COMM_WORLD, color, key, tag1, tag2, &newcomm);
  end = MPI_Wtime();
  print_comm(2, rank, newcomm);
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("lwgrp_comm_split_create time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  MPI_Comm_split(MPI_COMM_WORLD, color, key, &newcomm);
  end = MPI_Wtime();
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("MPI_Comm_split time %f secs\n", end - start);
  }



  color = 0;
  key = -rank;
  start = MPI_Wtime();
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, tag1, tag2, &size, members);
  end = MPI_Wtime();
  print_members(3, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  lwgrp_comm_split_create(MPI_COMM_WORLD, color, key, tag1, tag2, &newcomm);
  end = MPI_Wtime();
  print_comm(4, rank, newcomm);
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("lwgrp_comm_split_create time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  MPI_Comm_split(MPI_COMM_WORLD, color, key, &newcomm);
  end = MPI_Wtime();
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("MPI_Comm_split time %f secs\n", end - start);
  }


  color = rank;
  key = -rank;
  start = MPI_Wtime();
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, tag1, tag2, &size, members);
  end = MPI_Wtime();
  print_members(5, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  lwgrp_comm_split_create(MPI_COMM_WORLD, color, key, tag1, tag2, &newcomm);
  end = MPI_Wtime();
  print_comm(6, rank, newcomm);
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("lwgrp_comm_split_create time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  MPI_Comm_split(MPI_COMM_WORLD, color, key, &newcomm);
  end = MPI_Wtime();
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("MPI_Comm_split time %f secs\n", end - start);
  }


  color = (rank % 2) ? 0 : MPI_UNDEFINED;
  key = -rank;
  start = MPI_Wtime();
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, tag1, tag2, &size, members);
  end = MPI_Wtime();
  print_members(7, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  lwgrp_comm_split_create(MPI_COMM_WORLD, color, key, tag1, tag2, &newcomm);
  end = MPI_Wtime();
  print_comm(8, rank, newcomm);
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("lwgrp_comm_split_create time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  MPI_Comm_split(MPI_COMM_WORLD, color, key, &newcomm);
  end = MPI_Wtime();
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("MPI_Comm_split time %f secs\n", end - start);
  }


  color = rank % 2;
  key = rank;
  start = MPI_Wtime();
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, tag1, tag2, &size, members);
  end = MPI_Wtime();
  print_members(9, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  lwgrp_comm_split_create(MPI_COMM_WORLD, color, key, tag1, tag2, &newcomm);
  end = MPI_Wtime();
  print_comm(10, rank, newcomm);
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("lwgrp_comm_split_create time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  MPI_Comm_split(MPI_COMM_WORLD, color, key, &newcomm);
  end = MPI_Wtime();
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("MPI_Comm_split time %f secs\n", end - start);
  }


  color = rank / 4;
  key = rank;
  start = MPI_Wtime();
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, tag1, tag2, &size, members);
  end = MPI_Wtime();
  print_members(11, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  lwgrp_comm_split_create(MPI_COMM_WORLD, color, key, tag1, tag2, &newcomm);
  end = MPI_Wtime();
  print_comm(12, rank, newcomm);
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("lwgrp_comm_split_create time %f secs\n", end - start);
  }

  start = MPI_Wtime();
  MPI_Comm_split(MPI_COMM_WORLD, color, key, &newcomm);
  end = MPI_Wtime();
  if (newcomm != MPI_COMM_NULL) {
    MPI_Comm_free(&newcomm);
  }
  if (rank == 0) {
    printf("MPI_Comm_split time %f secs\n", end - start);
  }


  free(members);

  MPI_Finalize();

  return 0;
}
