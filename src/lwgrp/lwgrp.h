/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-568372.
 * All rights reserved.
 * This file is part of the LWGRP library.
 * For details, see https://github.com/hpc/lwgrp
 * Please also read this file: LICENSE.TXT. */

#ifndef _LWGRP_H
#define _LWGRP_H

#include "mpi.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#define LWGRP_SUCCESS (0)

extern int LWGRP_MSG_TAG_0;

/* The recommended interface for most cases is to use lwgrp_comm.
 * This abstraction is similar to an MPI communicator.
 *
 * Some users may want to invoke lower-level routines
 * using chains, rings, logchains, or logrings.
 *
 * We represent groups of processes using a doubly-linked list called
 * a "chain".  This is a struct that records the number of processes
 * in the group, the rank of the local process within the group, the
 * address of the local process, and the addresses of the processes
 * having ranks one less (left) and one more (right) than the local
 * process.  We implement this on MPI, so for addresses we record a
 * parent communicator and ranks within that communicator.  To be
 * lightweight, the reference to the communicator is a literal copy
 * of the handle value, not a dup. */

typedef struct lwgrp_chain {
  MPI_Comm comm;  /* communicator to send messages to procs in group */
  int comm_rank;  /* address (rank) of local process in communicator */
  int comm_left;  /* address (rank) of process whose group rank is one less than local */
  int comm_right; /* address (rank) of process whose group rank is one more than local */
  int group_size; /* number of processes in our group */
  int group_rank; /* our rank within the group [0,group_size) */
} lwgrp_chain;

/* We define a "ring", which is a chain where the endpoints wrap.
 * We use the same structure, but we define a different type
 * in case we want to extend this type in the future. */

typedef lwgrp_chain lwgrp_ring;

/* A logchain is a data structure that records the number and addresses
 * of processes that are 2^d ranks away from the local process to the
 * left and right sides, for d = 0 to ceiling(log N)-1.
 *
 * When multiple collectives are to be issued on a given chain, one
 * can construct and cache the logchain as an optimization.
 *
 * A logchain can be constructed by executing a collective operation on
 * a chain, or it can be filled in locally given a communicator as the
 * initial group.  It must often be used in conjunction with a chain
 * in communication operations. */

/* structure to associate a skip list of process ranks for a group */
typedef struct lwgrp_logchain {
  int  left_size;  /* number of elements in our left list */
  int  right_size; /* number of elements in our right list */
  int* left_list;  /* process addresses for 2^d hops to the left */
  int* right_list; /* process addresses for 2^d hops to the right */
} lwgrp_logchain;

/* again, we define a ring variant of the logchain */
typedef lwgrp_logchain lwgrp_logring;

/* We package a ring and logring into a single comm structure.
 * This object is provides routines closer to what people
 * expect from MPI communicators. */
typedef struct lwgrp_comm {
  lwgrp_ring  ring;
  lwgrp_logring logring;
} lwgrp_comm;

/* ---------------------------------
 * Methods to create and free chains
 * --------------------------------- */

int lwgrp_chain_set_null(lwgrp_chain* chain);
int lwgrp_chain_copy(const lwgrp_chain* in, lwgrp_chain* out);
int lwgrp_chain_build_from_ring(const lwgrp_ring* ring, lwgrp_chain* chain);
int lwgrp_chain_build_from_mpicomm(MPI_Comm comm, lwgrp_chain* chain);
int lwgrp_chain_build_from_vals(MPI_Comm comm, int left, int right, int size, int rank, lwgrp_chain* chain);
int lwgrp_chain_free(lwgrp_chain* chain);

/* ---------------------------------
 * Methods to create and free rings
 * --------------------------------- */

int lwgrp_ring_set_null(lwgrp_ring* ring);
int lwgrp_ring_copy(const lwgrp_ring* in, lwgrp_ring* out);
int lwgrp_ring_build_from_chain(const lwgrp_chain* chain, lwgrp_ring* ring);
int lwgrp_ring_build_from_mpicomm(MPI_Comm comm, lwgrp_ring* ring);
int lwgrp_ring_build_from_list(MPI_Comm comm, int size, const int ranklist[], lwgrp_ring* ring);
int lwgrp_ring_free(lwgrp_ring* ring);

/* ---------------------------------
 * Methods to create and free logchains
 * --------------------------------- */

int lwgrp_logchain_build_from_chain(const lwgrp_chain* chain, lwgrp_logchain* list);
int lwgrp_logchain_build_from_logring(const lwgrp_ring* ring, const lwgrp_logring* logring, lwgrp_logchain* list);
int lwgrp_logchain_build_from_mpicomm(MPI_Comm comm, lwgrp_logchain* list);
int lwgrp_logchain_free(lwgrp_logchain* list);

/* ---------------------------------
 * Methods to create and free logrings
 * --------------------------------- */

int lwgrp_logring_build_from_ring(const lwgrp_ring* ring, lwgrp_logring* list);
int lwgrp_logring_build_from_mpicomm(MPI_Comm comm, lwgrp_logring* list);
int lwgrp_logring_build_from_list(MPI_Comm, int size, const int ranklist[], lwgrp_logring* list);
int lwgrp_logring_free(lwgrp_logring* list);

/* ---------------------------------
 * Methods to create and free comms
 * --------------------------------- */

/* create a lwgrp comm from an MPI communicator */
int lwgrp_comm_build_from_mpicomm(
  MPI_Comm comm,      /* IN  - MPI communicator (handle) */
  lwgrp_comm* newcomm /* OUT - lwgrp communicator (pointer to comm struct) */
);

/* create a lwgrp comm from a lwgrp chain */
int lwgrp_comm_build_from_chain(
  const lwgrp_chain* chain, /* IN  - lwgrp chain (pointer to chain struct) */
  lwgrp_comm* newcomm       /* OUT - lwgrp communicator (pointer to comm struct) */
);

/* copy a lwgrp comm */
int lwgrp_comm_copy(
  const lwgrp_comm* comm, /* IN  - lwgrp chain (pointer to comm struct) */
  lwgrp_comm* newcomm     /* OUT - copy of lwgrp chain (pointer to comm struct) */
);

/* split a lwgrp comm into subcomms, where each subcomm holds all procs
 * in the same bin, takes O(B) space and O(B*log(N)) time where B is number
 * of bins and N is the number of procs in comm */
int lwgrp_comm_split_bin(
  const lwgrp_comm* comm, /* IN  - lwgrp communicator (pointer to comm struct) */
  int bins,               /* IN  - number of bins (non-negative integer) */
  int bin,                /* IN  - bin number 0 to bins-1, or -1  (integer) */
  lwgrp_comm* newcomm     /* OUT - lwgrp communicator of all procs in
                           *       the same bin */
);

/* implements semantics of MPI_Comm_split */
int lwgrp_comm_split(
  const lwgrp_comm* comm, /* IN  - lwgrp communicator (pointer to comm struct) */
  int color,              /* IN  - non-negative color value or MPI_UNDEFINED (integer) */
  int key,                /* IN  - key value to order ranks (integer) */
  lwgrp_comm* newcomm     /* OUT - lwgrp communicator of all procs with same color,
                           *       ordered by key, then rank in comm */
);

/* assigns an id to each unique string in the union of all strings of procs in comm */
int lwgrp_comm_rank_str(
  const lwgrp_comm* comm, /* IN  - lwgrp communicator (pointer to comm struct) */
  const char* str,        /* IN  - string to be ranked (string) */
  int* groups,            /* OUT - number of unique strings (non-negative integer) */
  int* groupid            /* OUT - rank of input string (non-negative integer) */
);

/* frees memory associated with comm structure */
int lwgrp_comm_free(
  lwgrp_comm* comm /* INOUT - lwgrp comm (pointer to comm struct) */
);

/* ---------------------------------
 * Collectives using chains
 * --------------------------------- */

/* given a specified number of bins, an index into those bins, and a
 * input group, create and return a new group consisting of all ranks
 * belonging to the same bin, my_bin should be in range [0,bins-1]
 * if my_bin < 0, then the empty group is returned */
int lwgrp_chain_split_bin_scan(
  int bins,                /* IN  - number of bins (non-negative integer) */
  int bin,                 /* IN  - bin to which calling proc belongs (integer) */
  const lwgrp_chain* in,   /* IN  - group to be split (handle) */
  lwgrp_chain* out         /* OUT - group containing all procs in same
                            *       bin as calling process (handle) */
);

/* execute a barrier over the chain */
int lwgrp_chain_barrier_dissemination(
  const lwgrp_chain* group /* IN  - group (handle) */
);

/* executes an allgather-like operation of a single integer */
int lwgrp_chain_allgather_brucks_int(
  int sendint,
  int recvbuf[],
  const lwgrp_chain* group
);

/* execute an allreduce over the chain */
int lwgrp_chain_allreduce_recursive(
  const void* inbuf,       /* IN  - input buffer for reduction */
  void* outbuf,            /* OUT - output buffer for reduction */
  int count,               /* IN  - number of elements in buffer
                            *       (non-negative integer) */
  MPI_Datatype type,       /* IN  - buffer datatype (handle) */
  MPI_Op op,               /* IN  - reduction operation (handle) */
  const lwgrp_chain* group /* IN  - group (handle) */
);

/* executes a left-to-right exclusive scan */
int lwgrp_chain_exscan_recursive(
  const void* sendbuf,     /* IN  - input buffer for scan (can be MPI_IN_PLACE) */
  void* recvbuf,           /* OUT - output buffer fo scan (not modified on rank 0) */
  int count,               /* IN  - number of elements in buffer (non negative integer) */
  MPI_Datatype type,       /* IN  - buffer datatype (handle) */
  MPI_Op op,               /* IN  - reduction operation (handle) */
  const lwgrp_chain* group /* IN  - group (handle) */
);

/* executes a left-to-right and right-to-left exclusive scan */
int lwgrp_chain_double_exscan_recursive(
  const void* sendleft,    /* IN  - input buffer for right-to-left exscan */
  void* recvright,         /* OUT - output buffer for right-to-left exscan */
  const void* sendright,   /* IN  - input buffer for left-to-right excan */
  void* recvleft,          /* OUT - output buffer for left-to-right exscan */
  int count,               /* IN  - number of elements in buffer
                            *       (non-negative integer) */
  MPI_Datatype type,       /* IN  - buffer datatype (handle) */
  MPI_Op op,               /* IN  - reduction operation (handle) */
  const lwgrp_chain* group /* IN  - group (handle) */
);

/* ---------------------------------
 * Collectives using logchains
 * --------------------------------- */

int lwgrp_logchain_reduce_recursive(
  const void* inbuf,         /* IN  - input buffer for reduction */
  void* outbuf,              /* OUT - output buffer for reduction */
  int count,                 /* IN  - number of elements in buffer
                              *       (non-negative integer) */
  MPI_Datatype type,         /* IN  - buffer datatype (handle) */
  MPI_Op op,                 /* IN  - reduction operation (handle) */
  int root,                  /* IN  - rank of root process (integer) */
  const lwgrp_chain* group,  /* IN  - group (handle) */
  const lwgrp_logchain* list /* IN  - list (handle) */
);

int lwgrp_logchain_allreduce_recursive(
  const void* inbuf,         /* IN  - input buffer for reduction */
  void* outbuf,              /* OUT - output buffer for reduction */
  int count,                 /* IN  - number of elements in buffer
                              *       (non-negative integer) */
  MPI_Datatype type,         /* IN  - buffer datatype (handle) */
  MPI_Op op,                 /* IN  - reduction operation (handle) */
  const lwgrp_chain* group,  /* IN  - group (handle) */
  const lwgrp_logchain* list /* IN  - list (handle) */
);

/* ---------------------------------
 * Collectives using rings
 * --------------------------------- */

/* given a specified number of bins, an index into those bins, and a
 * input group, create and return a new group consisting of all ranks
 * belonging to the same bin, my_bin should be in range [0,bins-1]
 * if my_bin < 0, then the empty group is returned */
int lwgrp_ring_split_bin_scan(
  int bins,               /* IN  - number of bins (non-negative integer) */
  int bin,                /* IN  - bin to which calling proc belongs (integer) */
  const lwgrp_ring* in,   /* IN  - group to be split (handle) */
  lwgrp_ring* out         /* OUT - group containing all procs in same
                           *       bin as calling process (handle) */
);

/* chunks split operation into pieces */
int lwgrp_ring_split_bin_radix(
  int bins,               /* IN  - number of bins (non-negative integer) */
  int bin,                /* IN  - bin to which calling proc belongs (integer) */
  const lwgrp_ring* in,   /* IN  - group to be split (handle) */
  lwgrp_ring* out         /* OUT - group containing all procs in same
                           *       bin as calling process (handle) */
);

int lwgrp_ring_alltoallv_linear(
  const void* sendbuf,    /* IN  - starting address of send buffer */
  const int sendcounts[], /* IN  - non-negative integer array (of length group size) specifying
                                   the number of elements to send to each processor */
  const int senddispls[], /* IN  - integer array (of length group size).  Entry j specifies
                                   the displacement (relative to sendbuf) from which to take
                                   the outgoing data destined for process j */
  void* recvbuf,          /* OUT - address of receive buffer */
  const int recvcounts[], /* IN  - non-negative integer array (of length group size) specifying
                                   the number of elements that can be received from each processor */
  const int recvdispls[], /* IN  - integer array (of length group size).  Entry i specifies
                                   the displacement (relative to recvbuf) at which to palce the
                                   incoming data from process i */
  MPI_Datatype recvtype,  /* IN  - data type of receive buffer elements (handle) */
  const lwgrp_ring* group /* IN  - group (handle) */
);

/* ---------------------------------
 * Collectives using logrings
 * --------------------------------- */

/* execute a barrier over the chain */
int lwgrp_logring_barrier_dissemination(
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_bcast_binomial(
  void* buffer,             /* IN  - send buffer (on root), receive buffer otherwise */
  int count,                /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype datatype,    /* IN  - data type of buffer elements (handle) */
  int root,                 /* IN  - rank of root process (integer) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

/* TODO: problem here in MPI is that intermediate ranks may use
 * a different type for which we can't just append lots of datatypes
 * in a temporary buffer */
int lwgrp_logring_gather_brucks(
  const void* sendbuf,      /* IN  - send buffer */
  void* recvbuf,            /* OUT - recive buffer */
  int num,                  /* IN  - number of elements on each process (non-negative integer) */
  MPI_Datatype datatype,    /* IN  - element datatype (handle) */
  int root,                 /* IN  - rank of root process (integer) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_allgather_brucks(
  const void* sendbuf,      /* IN  - send buffer */
  void* recvbuf,            /* OUT - recive buffer */
  int num,                  /* IN  - number of elements on each process (non-negative integer) */
  MPI_Datatype datatype,    /* IN  - element datatype (handle) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_allgatherv_brucks(
  const void* sendbuf,      /* IN  - send buffer */
  void* recvbuf,            /* OUT - recive buffer */
  const int counts[],       /* IN  - non-negative integer array (of length group size) specifying
                                     the number of elements each processor has */
  const int displs[],       /* IN  - integer array (of length group size).  Entry i specifies
                                     the displacement (relative to recvbuf) at which to palce the
                                     incoming data from process i */
  MPI_Datatype datatype,    /* IN  - element datatype (handle) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_alltoall_brucks(
  const void* sendbuf,      /* IN  - starting address of send buffer */
  void* recvbuf,            /* OUT - address of receive buffer */
  int num,                  /* IN  - number of elements sent to each process (non-negative integer) */
  MPI_Datatype datatype,    /* IN  - data type of buffer elements (handle) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_alltoallv_linear(
  const void* sendbuf,      /* IN  - starting address of send buffer */
  const int sendcounts[],   /* IN  - non-negative integer array (of length group size) specifying
                                     the number of elements to send to each processor */
  const int senddispls[],   /* IN  - integer array (of length group size).  Entry j specifies
                                     the displacement (relative to sendbuf) from which to take
                                     the outgoing data destined for process j */
  void* recvbuf,            /* OUT - address of receive buffer */
  const int recvcounts[],   /* IN  - non-negative integer array (of length group size) specifying
                                     the number of elements that can be received from each processor */
  const int recvdispls[],   /* IN  - integer array (of length group size).  Entry i specifies
                                     the displacement (relative to recvbuf) at which to palce the
                                     incoming data from process i */
  MPI_Datatype datatype,    /* IN  - data type of buffer elements (handle) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_reduce_recursive(
  const void* inbuf,         /* IN  - input buffer for reduction */
  void* outbuf,              /* OUT - output buffer for reduction */
  int count,                 /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,         /* IN  - buffer datatype (handle) */
  MPI_Op op,                 /* IN  - reduction operation (handle) */
  int root,                  /* IN  - rank of root process (integer) */
  const lwgrp_ring* group,   /* IN  - group (handle) */
  const lwgrp_logring* list  /* IN  - list (handle) */
);

int lwgrp_logring_allreduce_recursive(
  const void* inbuf,        /* IN  - input buffer for reduction */
  void* outbuf,             /* OUT - output buffer for reduction */
  int count,                /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,        /* IN  - buffer datatype (handle) */
  MPI_Op op,                /* IN  - reduction operation (handle) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_scan_recursive(
  const void* inbuf,        /* IN  - input buffer for reduction */
  void* outbuf,             /* OUT - output buffer for reduction */
  int count,                /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,        /* IN  - buffer datatype (handle) */
  MPI_Op op,                /* IN  - reduction operation (handle) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_exscan_recursive(
  const void* inbuf,        /* IN  - input buffer for reduction */
  void* outbuf,             /* OUT - output buffer for reduction */
  int count,                /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,        /* IN  - buffer datatype (handle) */
  MPI_Op op,                /* IN  - reduction operation (handle) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

int lwgrp_logring_double_exscan_recursive(
  const void* sendleft,     /* IN  - input buffer for right-to-left exscan */
  void* recvright,          /* OUT - output buffer for right-to-left exscan */
  const void* sendright,    /* IN  - input buffer for left-to-right excan */
  void* recvleft,           /* OUT - output buffer for left-to-right exscan */
  int count,                /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,        /* IN  - buffer datatype (handle) */
  MPI_Op op,                /* IN  - reduction operation (handle) */
  const lwgrp_ring* group,  /* IN  - group (handle) */
  const lwgrp_logring* list /* IN  - list (handle) */
);

/* ---------------------------------
 * Comm Query routines
 * --------------------------------- */

/* get our rank within the communicator */
int lwgrp_comm_rank(
  const lwgrp_comm* comm, /* IN  - communicator (pointer to comm struct) */
  int* rank               /* OUT - rank within communicator (integer) */
);

/* get size of the communicator */
int lwgrp_comm_size(
  const lwgrp_comm* comm, /* IN  - communicator (pointer to comm struct) */
  int* size               /* OUT - size of communicator (integer) */
);

/* ---------------------------------
 * Collectives using comms
 * --------------------------------- */

int lwgrp_comm_barrier(
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_bcast(
  void* buffer,          /* IN  - send buffer (on root), receive buffer otherwise */
  int count,             /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype datatype, /* IN  - data type of buffer elements (handle) */
  int root,              /* IN  - rank of root process (integer) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_gather(
  const void* sendbuf,   /* IN  - send buffer */
  void* recvbuf,         /* OUT - recive buffer */
  int num,               /* IN  - number of elements on each process (non-negative integer) */
  MPI_Datatype datatype, /* IN  - element datatype (handle) */
  int root,              /* IN  - rank of root process (integer) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_allgather(
  const void* sendbuf,   /* IN  - send buffer */
  void* recvbuf,         /* OUT - recive buffer */
  int num,               /* IN  - number of elements on each process (non-negative integer) */
  MPI_Datatype datatype, /* IN  - element datatype (handle) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_allgatherv(
  const void* sendbuf,   /* IN  - send buffer */
  void* recvbuf,         /* OUT - recive buffer */
  const int counts[],    /* IN  - non-negative integer array (of length group size) specifying
                                  the number of elements each processor has */
  const int displs[],    /* IN  - integer array (of length group size).  Entry i specifies
                                  the displacement (relative to recvbuf) at which to palce the
                                  incoming data from process i */
  MPI_Datatype datatype, /* IN  - element datatype (handle) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_alltoall(
  const void* sendbuf,   /* IN  - send buffer */
  void* recvbuf,         /* OUT - recive buffer */
  int num,               /* IN  - number of elements on each process (non-negative integer) */
  MPI_Datatype datatype, /* IN  - element datatype (handle) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_alltoallv(
  const void* sendbuf,    /* IN  - send buffer */
  const int sendcounts[], /* IN  - non-negative integer array (of length group size) specifying
                                   the number of elements to send to each processor */
  const int senddispls[], /* IN  - integer array (of length group size).  Entry j specifies
                                   the displacement (relative to sendbuf) from which to take
                                   the outgoing data destined for process j */
  void* recvbuf,          /* OUT - recive buffer */
  const int recvcounts[], /* IN  - non-negative integer array (of length group size) specifying
                                   the number of elements that can be received from each processor */
  const int recvdispls[], /* IN  - integer array (of length group size).  Entry i specifies
                                   the displacement (relative to recvbuf) at which to palce the
                                   incoming data from process i */
  MPI_Datatype datatype,  /* IN  - element datatype (handle) */
  const lwgrp_comm* comm  /* IN  - group (handle) */
);

int lwgrp_comm_reduce(
  const void* inbuf,     /* IN  - input buffer for reduction */
  void* outbuf,          /* OUT - output buffer for reduction */
  int count,             /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,     /* IN  - buffer datatype (handle) */
  MPI_Op op,             /* IN  - reduction operation (handle) */
  int root,              /* IN  - rank of root process (integer) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_allreduce(
  const void* inbuf,     /* IN  - input buffer for reduction */
  void* outbuf,          /* OUT - output buffer for reduction */
  int count,             /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,     /* IN  - buffer datatype (handle) */
  MPI_Op op,             /* IN  - reduction operation (handle) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_scan(
  const void* inbuf,     /* IN  - input buffer for reduction */
  void* outbuf,          /* OUT - output buffer for reduction */
  int count,             /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,     /* IN  - buffer datatype (handle) */
  MPI_Op op,             /* IN  - reduction operation (handle) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_exscan(
  const void* inbuf,     /* IN  - input buffer for reduction */
  void* outbuf,          /* OUT - output buffer for reduction */
  int count,             /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,     /* IN  - buffer datatype (handle) */
  MPI_Op op,             /* IN  - reduction operation (handle) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

int lwgrp_comm_double_exscan(
  const void* sendleft,  /* IN  - input buffer for right-to-left exscan */
  void* recvright,       /* OUT - output buffer for right-to-left exscan */
  const void* sendright, /* IN  - input buffer for left-to-right excan */
  void* recvleft,        /* OUT - output buffer for left-to-right exscan */
  int count,             /* IN  - number of elements in buffer (non-negative integer) */
  MPI_Datatype type,     /* IN  - buffer datatype (handle) */
  MPI_Op op,             /* IN  - reduction operation (handle) */
  const lwgrp_comm* comm /* IN  - group (handle) */
);

#ifdef __cplusplus
}
#endif /* __cplusplus */
#endif /* _LWGRP_H */
