/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#include "scr_globals.h"

/*
=========================================
Functions to send/recv strings
=========================================
*/

/* sends a NUL-terminated string to a process,
 * which may be MPI_PROC_NULL */
int scr_str_send(const char* str, int rank, MPI_Comm comm)
{
  /* get length of string */
  int len = 0;
  if (str != NULL) {
    len = strlen(str) + 1;
  }

  /* send the length of the string */
  MPI_Send(&len, 1, MPI_INT, rank, 999, comm);

  /* if the length is positive, send the string */
  if (len > 0) {
    MPI_Send((void*) str, len, MPI_CHAR, rank, 999, comm);
  }

  return SCR_SUCCESS;
}

/* recieves a NUL-terminated string from a process,
 * and returns pointer to newly allocated string,
 * returns NULL if rank is MPI_PROC_NULL */
int scr_str_recv(char** str, int rank, MPI_Comm comm)
{
  MPI_Status status;

  /* receive length of string */
  int len = 0;
  if (rank != MPI_PROC_NULL) {
    MPI_Recv(&len, 1, MPI_INT, rank, 999, comm, &status);
  }

  /* if length is positive, receive the string */
  char* recvstr = NULL;
  if (len > 0) {
    /* allocate space to receive string */
    recvstr = (char*) malloc(len);
    if (recvstr == NULL) {
      scr_abort(-1, "Failed to allocate %d bytes to receive string @ %s:%d",
        len, __FILE__, __LINE__
      );
    }

    /* receive the string */
    MPI_Recv(recvstr, len, MPI_CHAR, rank, 999, comm, &status);
  }

  /* return address of allocated string in caller's pointer */
  if (str != NULL) {
    *str = recvstr;
  } else {
    scr_abort(-1, "Given invalid pointer to record address of allocated string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  return SCR_SUCCESS;
}

/* sends a NUL-terminated string to a process,
 * allocates space and recieves a NUL-terminated string from a process,
 * can specify MPI_PROC_NULL as either send or recv rank */
int scr_str_sendrecv(
  const char* send_str, int send_rank,
  char** recv_str,      int recv_rank,
  MPI_Comm comm)
{
  MPI_Status status;

  /* get length of our send string */
  int send_len = 0;
  if (send_str != NULL) {
    send_len = strlen(send_str) + 1;
  }

  /* exchange length of strings, note that we initialize recv_len
   * so that it's valid if we recieve from MPI_PROC_NULL */
  int recv_len = 0;
  MPI_Sendrecv(
    &send_len, 1, MPI_INT, send_rank, 999,
    &recv_len, 1, MPI_INT, recv_rank, 999,
    comm, &status
  );

  /* if receive length is positive, allocate space to receive string */
  char* tmp_str = NULL;
  if (recv_len > 0) {
    tmp_str = (char*) malloc(recv_len);
    if (tmp_str == NULL) {
      scr_abort(-1, "Failed to allocate %d bytes to receive string @ %s:%d",
        recv_len, __FILE__, __LINE__
      );
    }
  }

  /* exchange strings */
  MPI_Sendrecv(
    (void*) send_str, send_len, MPI_CHAR, send_rank, 999,
    (void*) tmp_str,  recv_len, MPI_CHAR, recv_rank, 999,
    comm, &status
  );

  /* return address of allocated string in caller's pointer */
  *recv_str = tmp_str;
  return SCR_SUCCESS;
}

/* broadcast a string from root process,
 * newly allocated string is returned in str on all non-root procs */
int scr_str_bcast(char** str, int root, MPI_Comm comm)
{
  /* get our rank in comm */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* if we are root, set the length */
  int len = 0;
  if (rank == root && str != NULL && *str != NULL) {
    len = strlen(*str) + 1;
  }

  /* broadcast the length */
  MPI_Bcast(&len, 1, MPI_INT, root, comm);

  /* allocate space to receive string */
  char* tmp_str = NULL;
  if (rank == root) {
    tmp_str = *str;
  } else {
    if (len > 0) {
      tmp_str = (char*) malloc(len);
      if (tmp_str == NULL) {
        scr_abort(-1, "Failed to allocate %d bytes to bcast string @ %s:%d",
          len, __FILE__, __LINE__
        );
      }
    }
  }

  /* broadcast the string */
  MPI_Bcast(tmp_str, len, MPI_CHAR, root, comm);

  /* if we are not the root, return allocated string in caller's pointer */
  if (rank != root) {
    *str = tmp_str;
  }

  return SCR_SUCCESS;
}

/* broadcast a string from root process into buffer provided by
 * caller, checks that string is size bytes or less */
int scr_strn_bcast(char* str, int n, int root, MPI_Comm comm)
{
  /* we don't handle NULL strings */
  if (str == NULL) {
    scr_abort(-1, "Can't bcast a NULL string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* get our rank in comm */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* if we are root, set the length */
  int len = 0;
  if (rank == root && str != NULL) {
    len = strlen(str) + 1;
  }

  /* broadcast the length */
  MPI_Bcast(&len, 1, MPI_INT, root, comm);

  /* check that our buffer is big enough to receive incoming string */
  if (len > n || n < 0) {
    scr_abort(-1, "String buffer of %d bytes too short for %d byte string @ %s:%d",
      n, len, __FILE__, __LINE__
    );
  }

  /* broadcast the string */
  MPI_Bcast(str, len, MPI_CHAR, root, comm);

  return SCR_SUCCESS;
}

/*
=========================================
MPI utility functions
=========================================
*/

/* returns true (non-zero) if flag on each process in
 * scr_comm_world is true */
int scr_alltrue(int flag)
{
  int all_true = 0;
  MPI_Allreduce(&flag, &all_true, 1, MPI_INT, MPI_LAND, scr_comm_world);
  return all_true;
}

/* given a comm as input, find the left and right partner
 * ranks and hostnames */
int scr_set_partners(
  MPI_Comm comm, int dist,
  int* lhs_rank, int* lhs_rank_world, char** lhs_hostname,
  int* rhs_rank, int* rhs_rank_world, char** rhs_hostname)
{
  /* find our position in the communicator */
  int my_rank, ranks;
  MPI_Comm_rank(comm, &my_rank);
  MPI_Comm_size(comm, &ranks);

  /* shift parter distance to a valid range */
  while (dist > ranks) {
    dist -= ranks;
  }
  while (dist < 0) {
    dist += ranks;
  }

  /* compute ranks to our left and right partners */
  int lhs = (my_rank + ranks - dist) % ranks;
  int rhs = (my_rank + ranks + dist) % ranks;
  (*lhs_rank) = lhs;
  (*rhs_rank) = rhs;

  /* shift hostnames to the right */
  scr_str_sendrecv(scr_my_hostname, rhs, lhs_hostname, lhs, comm);

  /* shift hostnames to the left */
  scr_str_sendrecv(scr_my_hostname, lhs, rhs_hostname, rhs, comm);

  MPI_Request request[2];
  MPI_Status  status[2];

  /* shift rank in scr_comm_world to the right */
  MPI_Irecv(lhs_rank_world,     1, MPI_INT, lhs, 0, comm, &request[0]);
  MPI_Isend(&scr_my_rank_world, 1, MPI_INT, rhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift rank in scr_comm_world to the left */
  MPI_Irecv(rhs_rank_world,     1, MPI_INT, rhs, 0, comm, &request[0]);
  MPI_Isend(&scr_my_rank_world, 1, MPI_INT, lhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  return SCR_SUCCESS;
}
