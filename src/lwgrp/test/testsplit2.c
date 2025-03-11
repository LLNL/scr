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

int lwgrp_comm_split_bin_members(MPI_Comm mpicomm, int bins, int bin, int* size, int members[])
{
  lwgrp_comm comm;
  lwgrp_comm_build_from_mpicomm(mpicomm, &comm);

  lwgrp_comm newcomm;
  lwgrp_comm_split_bin(&comm, bins, bin, &newcomm);

  lwgrp_comm_size(&newcomm, size);

  if (*size > 0) {
    int rank;
    //lwgrp_comm_rank(&newcomm, &rank);
    lwgrp_comm_rank(&comm, &rank);
    lwgrp_comm_allgather(&rank, members, 1, MPI_INT, &newcomm);
  }

  lwgrp_comm_free(&newcomm);
  lwgrp_comm_free(&comm);
}

int lwgrp_comm_split_members(MPI_Comm mpicomm, int color, int key, int* size, int members[])
{
  lwgrp_comm comm;
  lwgrp_comm_build_from_mpicomm(mpicomm, &comm);

  lwgrp_comm newcomm;
  lwgrp_comm_split(&comm, color, key, &newcomm);

  lwgrp_comm_size(&newcomm, size);

  if (*size > 0) {
    int rank;
    //lwgrp_comm_rank(&newcomm, &rank);
    lwgrp_comm_rank(&comm, &rank);
    lwgrp_comm_allgather(&rank, members, 1, MPI_INT, &newcomm);
  }

  lwgrp_comm_free(&newcomm);
  lwgrp_comm_free(&comm);
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

  int* inbuf  = (int*) malloc(ranks * sizeof(int));
  int* outbuf = (int*) malloc(ranks * sizeof(int));
  int i;
  for (i = 0; i < ranks; i++) {
    inbuf[i]  = rank * ranks + i;
    outbuf[i] = -1;
  }

  color = 0;
  key = rank;
  start = MPI_Wtime();
//  lwgrp_comm_split_bin_members(MPI_COMM_WORLD, 1, color, &size, members);
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, &size, members);
  end = MPI_Wtime();
  print_members(1, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
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
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, &size, members);
  end = MPI_Wtime();
  print_members(3, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
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
//  lwgrp_comm_split_bin_members(MPI_COMM_WORLD, ranks, color, &size, members);
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, &size, members);
  end = MPI_Wtime();
  print_members(5, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
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
//  lwgrp_comm_split_bin_members(MPI_COMM_WORLD, 1, color, &size, members);
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, &size, members);
  end = MPI_Wtime();
  print_members(7, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
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
//  lwgrp_comm_split_bin_members(MPI_COMM_WORLD, 2, color, &size, members);
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, &size, members);
  end = MPI_Wtime();
  print_members(9, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
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
  lwgrp_comm_split_members(MPI_COMM_WORLD, color, key, &size, members);
  end = MPI_Wtime();
  print_members(11, rank, size, members);
  if (rank == 0) {
    printf("lwgrp_comm_split_members time %f secs\n", end - start);
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


  lwgrp_comm comm_world;
  lwgrp_comm_build_from_mpicomm(MPI_COMM_WORLD, &comm_world);
  int groups, groupid;
  char hostname[256];
  gethostname(hostname, sizeof(hostname));
  lwgrp_comm_rank_str(&comm_world, hostname, &groups, &groupid);
  printf("%d: %s: %d of %d\n", rank, hostname, groupid, groups);


  free(members);

  MPI_Finalize();

  return 0;
}
