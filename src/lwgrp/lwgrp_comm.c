#include "mpi.h"
#include "lwgrp.h"
#include "lwgrp_internal.h"

/* ---------------------------------
 * Constructors / destructors
 * --------------------------------- */

int lwgrp_comm_build_from_mpicomm(
  MPI_Comm comm,
  lwgrp_comm* newcomm)
{
  /* these are both local, so the whole op is local */
  lwgrp_ring_build_from_mpicomm(comm, &newcomm->ring);
  lwgrp_logring_build_from_mpicomm(comm, &newcomm->logring);
  return LWGRP_SUCCESS;
}

int lwgrp_comm_build_from_chain(
  const lwgrp_chain* chain,
  lwgrp_comm* newcomm)
{
  lwgrp_ring_build_from_chain(chain, &newcomm->ring);
  lwgrp_logring_build_from_ring(&newcomm->ring, &newcomm->logring);
  return LWGRP_SUCCESS;
}
  
#if 0
int lwgrp_comm_copy(
  const lwgrp_comm* comm,
  lwgrp_comm* newcomm)
{
  /* this is all local */
  lwgrp_ring_copy(&comm->ring, &newcomm->ring);
  lwgrp_logring_copy(&comm->logring, &newcomm->logring);
  return LWGRP_SUCCESS;
}
#endif

int lwgrp_comm_split_bin(
  const lwgrp_comm* comm,
  int bins,
  int bin,
  lwgrp_comm* newcomm)
{
  /* TODO: if many bins, do this in phases or call bitonic split */
  lwgrp_ring_split_bin_radix(bins, bin, &comm->ring, &newcomm->ring);
  lwgrp_logring_build_from_ring(&newcomm->ring, &newcomm->logring);
  return LWGRP_SUCCESS;
}

int lwgrp_comm_free(lwgrp_comm* comm)
{
  lwgrp_logring_free(&comm->logring);
  lwgrp_ring_free(&comm->ring);
  return LWGRP_SUCCESS;
}

/* ---------------------------------
 * Query routines
 * --------------------------------- */

int lwgrp_comm_rank(
  const lwgrp_comm* comm,
  int* rank)
{
  const lwgrp_ring* ring = &comm->ring;
  *rank = ring->group_rank;
  return LWGRP_SUCCESS;
}

int lwgrp_comm_size(
  const lwgrp_comm* comm,
  int* size)
{
  const lwgrp_ring* ring = &comm->ring;
  *size = ring->group_size;
  return LWGRP_SUCCESS;
}

/* ---------------------------------
 * Collectives
 * --------------------------------- */

int lwgrp_comm_barrier(const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_barrier_dissemination(&comm->ring, &comm->logring);
  return rc;
}

int lwgrp_comm_bcast(
  void* buffer,
  int count,
  MPI_Datatype datatype,
  int root,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_bcast_binomial(
    buffer, count, datatype, root,
    &comm->ring, &comm->logring
  );
  return rc;
} 

int lwgrp_comm_gather(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype datatype,
  int root,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_gather_brucks(
    sendbuf, recvbuf, count, datatype,
    root, &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_allgather(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype datatype,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_allgather_brucks(
    sendbuf, recvbuf, count, datatype,
    &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_allgatherv(
  const void* sendbuf,
  void* recvbuf,
  const int counts[],
  const int displs[],
  MPI_Datatype datatype,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_allgatherv_brucks(
    sendbuf, recvbuf, counts, displs, datatype,
    &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_alltoall(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype datatype,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_alltoall_brucks(
    sendbuf, recvbuf, count, datatype,
    &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_alltoallv(
  const void* sendbuf,
  const int sendcounts[],
  const int senddispls[],
  void* recvbuf,
  const int recvcounts[],
  const int recvdispls[],
  MPI_Datatype datatype,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_alltoallv_linear(
    sendbuf, sendcounts, senddispls, recvbuf, recvcounts, recvdispls, datatype,
    &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_allreduce(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype datatype,
  MPI_Op op,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_allreduce_recursive(
    sendbuf, recvbuf, count, datatype, op,
    &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_reduce(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype datatype,
  MPI_Op op,
  int root,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_reduce_recursive(
    sendbuf, recvbuf, count, datatype, op, root,
    &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_scan(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype datatype,
  MPI_Op op,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_scan_recursive(
    sendbuf, recvbuf, count, datatype, op,
    &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_exscan(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype datatype,
  MPI_Op op,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_exscan_recursive(
    sendbuf, recvbuf, count, datatype, op,
    &comm->ring, &comm->logring
  );
  return rc;
}

int lwgrp_comm_double_exscan(
  const void* sendleft,
  void* recvright,
  const void* sendright,
  void* recvleft,
  int count,
  MPI_Datatype datatype,
  MPI_Op op,
  const lwgrp_comm* comm)
{
  int rc = lwgrp_logring_double_exscan_recursive(
    sendleft, recvright, sendright, recvleft,
    count, datatype, op,
    &comm->ring, &comm->logring
  );
  return rc;
}
