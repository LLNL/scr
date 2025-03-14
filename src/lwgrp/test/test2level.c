// mpicc -g -O0 -o test2level test2level.c -I../install/include -L../install/lib -llwgrp
//
// bug 1: lwgrp_ring_ops.c mixed send/recv in split_bin when last step wraps
//        so that source and dest are the same rank (needed to flip order of
//        one send/recv pair)
// bug 2: split_bin_2level need to use size of parent comm as invalid rank rather than max of
//        lev1 comm in reduce
// bug 3: split_bin_2level mixed order of params in call to MPI_Type_contiguous
// bug 4: lwgrp_chain_ops.c was using group_rank instead of comm_rank in
//        calls to lwgrp_memcpy
// bug 5: split_bin_2level need to use scan_type in bcast
// bug 6: split_bin_2level initialize recv values from scan if on end of lev2 group
// bug 7: lwgrp_chain_ops.c forgot to multiple extent by count in double_exscan
// bug 8: messed up next rank computation in scan operation
// bug 9: need to initialize structs to empty groups for procs with bin=-1

#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "lwgrp.h"

#define SCAN_RANK  (0)
#define SCAN_COUNT (1)

/* user-defined reduction operation to compute min/max/sum */
static void scan_chain(void* invec, void* inoutvec, int* len, MPI_Datatype* type)
{
   int* a = (int*) invec;
   int* b = (int*) inoutvec;

   int i;
   for (i = 0; i < *len; i++) {
     /* if we haven't already set the rank, take the value from the a */
     if (b[SCAN_RANK] == MPI_PROC_NULL) {
       b[SCAN_RANK] = a[SCAN_RANK];
     }

     /* add a's count to b */
     b[SCAN_COUNT] += a[SCAN_COUNT];

     /* advance to next element */
     a += 2;
     b += 2;
  }
}

int split_bin_2level(
  int bins,
  int bin,
  const lwgrp_ring* lev1_ring,
  const lwgrp_logring* lev1_logring,
  const lwgrp_ring* lev2_ring,
  const lwgrp_logring* lev2_logring,
  lwgrp_ring* new_lev1_ring,
  lwgrp_logring* new_lev1_logring,
  lwgrp_ring* new_lev2_ring,
  lwgrp_logring* new_lev2_logring)
{
  int i;

  /* initialize new rings and logrings to empty groups,
   * we'll overwrite these if proc is really in a group */
  lwgrp_ring_set_null(new_lev1_ring);
  lwgrp_ring_set_null(new_lev2_ring);
  lwgrp_logring_build_from_ring(new_lev1_ring, new_lev1_logring);
  lwgrp_logring_build_from_ring(new_lev2_ring, new_lev2_logring);

  if (bins <= 0) {
    return 0;
  }

  /* get our rank within and the size of the parent communicator */
  int comm_size;
  int comm_rank = lev1_ring->comm_rank;
  MPI_Comm_size(lev1_ring->comm, &comm_size);

  /* allocate memory to execute collectives */
  int* reduce_inbuf   = (int*) malloc(bins * sizeof(int));
  int* reduce_outbuf  = (int*) malloc(bins * sizeof(int));
  int* scan_inbuf     = (int*) malloc(2 * bins * sizeof(int));
  int* scan_recvleft  = (int*) malloc(2 * bins * sizeof(int));
  int* scan_recvright = (int*) malloc(2 * bins * sizeof(int));

  /* intiaize all bins to MPI_PROC_NULL, except for our
   * bin in which case we list our rank within comm */
  for (i = 0; i < bins; i++) {
    /* initialize all bins to size(lev1), would like MPI_PROC_NULL,
     * but we use size instead so that reduce(min) does the right thing */
    reduce_inbuf[i] = comm_size;
  }
  if (bin >= 0) {
    reduce_inbuf[bin] = comm_rank;
  }

  /* reduce to node leader to find lowest rank in each bin */
  lwgrp_logring_reduce(
    reduce_inbuf, reduce_outbuf, bins, MPI_INT, MPI_MIN,
    0, lev1_ring, lev1_logring
  );

  /* create the scan type (a rank and a count pair) */
  MPI_Datatype scan_type;
  MPI_Type_contiguous(2, MPI_INT, &scan_type);
  MPI_Type_commit(&scan_type);

  /* double exscan across node leaders to
   * build info for new node leader chains */
  int lev1_rank = lev1_ring->group_rank;
  if (lev1_rank == 0) {
    /* prepare data for input to double scan, for each bin
     * record the lowest rank and a count of either 0 or 1 */
    for (i = 0; i < bins; i++) {
      if (reduce_outbuf[i] != comm_size) {
        scan_inbuf[i*2 + SCAN_RANK]  = reduce_outbuf[i];
        scan_inbuf[i*2 + SCAN_COUNT] = 1;
      } else {
        scan_inbuf[i*2 + SCAN_RANK]  = MPI_PROC_NULL;
        scan_inbuf[i*2 + SCAN_COUNT] = 0;
      }
    }

    /* create the scan operation */
    MPI_Op scan_op;
    int commutative = 0;
    MPI_Op_create(scan_chain, commutative, &scan_op);

    /* execute the double exclusive scan to get next rank and
     * count of ranks to either side for each bin */
    lwgrp_logring_double_exscan(
      scan_inbuf, scan_recvright, scan_inbuf, scan_recvleft,
      bins, scan_type, scan_op, lev2_ring, lev2_logring
    );

    /* if we're on the end of the level 2 group, need to initialize
     * the recv values */
    int lev2_rank = lev2_ring->group_rank;
    int lev2_size = lev2_ring->group_size;
    if (lev2_rank == 0) {
      /* we're on the left end of lev2 group, so we didn't get
       * anything from the left side */
      for (i = 0; i < bins; i++) {
        scan_recvleft[i*2 + SCAN_RANK]  = MPI_PROC_NULL;
        scan_recvleft[i*2 + SCAN_COUNT] = 0;
      }
    }
    if (lev2_rank == lev2_size-1) {
      /* we're on the right end of lev2 group, so we didn't get
       * anything from the right side */
      for (i = 0; i < bins; i++) {
        scan_recvright[i*2 + SCAN_RANK]  = MPI_PROC_NULL;
        scan_recvright[i*2 + SCAN_COUNT] = 0;
      }
    }

    /* free the scan op */
    MPI_Op_free(&scan_op);
  }

  /* broadcast scan results to local comm */
  lwgrp_logring_bcast(scan_recvleft,  bins, scan_type, 0, lev1_ring, lev1_logring);
  lwgrp_logring_bcast(scan_recvright, bins, scan_type, 0, lev1_ring, lev1_logring);

  /* free the scan type */
  MPI_Type_free(&scan_type);

  /* call bin_split on local chain */
  lwgrp_ring_split_bin_radix(bins, bin, lev1_ring, new_lev1_ring);
  lwgrp_logring_build_from_ring(new_lev1_ring, new_lev1_logring);

  /* for each valid bin, all rank 0 procs of new lev1 groups form new lev2 groups */
  if (bin >= 0) {
    int new_lev1_rank = new_lev1_ring->group_rank;
    if (new_lev1_rank == 0) {
      /* extract chain values from scan results */
      MPI_Comm comm = new_lev1_ring->comm;
      int left  = scan_recvleft[2*bin  + SCAN_RANK];
      int right = scan_recvright[2*bin + SCAN_RANK];
      int size  = scan_recvleft[2*bin + SCAN_COUNT] + scan_recvright[2*bin + SCAN_COUNT] + 1;
      int rank  = scan_recvleft[2*bin + SCAN_COUNT];

      /* build chain, then ring, then logring, and finally free chain */
      lwgrp_chain tmp_chain;
      lwgrp_chain_build_from_vals(comm, left, right, size, rank, &tmp_chain);
      lwgrp_ring_build_from_chain(&tmp_chain, new_lev2_ring);
      lwgrp_logring_build_from_ring(new_lev2_ring, new_lev2_logring);
      lwgrp_chain_free(&tmp_chain);
    }
  }

  /* free our temporary memory */
  free(scan_recvright);
  free(scan_recvleft);
  free(scan_inbuf);
  free(reduce_outbuf);
  free(reduce_inbuf);

  return 0;
}

int main(int argc, char* argv[])
{
  MPI_Init(NULL, NULL);

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  lwgrp_ring lev1_ring, lev2_ring;
  lwgrp_logring lev1_logring, lev2_logring;

  lwgrp_ring world_ring;
  lwgrp_ring_build_from_comm(MPI_COMM_WORLD, &world_ring);

  int bin, bins;
  bins = ranks / 2;
  bin  = rank  / 2;
  lwgrp_ring_split_bin_radix(bins, bin, &world_ring, &lev1_ring);

  bin = -1;
  if (rank % 2 == 0) {
    bin = 0;
  }
  lwgrp_ring_split_bin_scan(1, bin, &world_ring, &lev2_ring);

  lwgrp_logring_build_from_ring(&lev1_ring, &lev1_logring);
  lwgrp_logring_build_from_ring(&lev2_ring, &lev2_logring);

  lwgrp_ring new_lev1_ring, new_lev2_ring;
  lwgrp_logring new_lev1_logring, new_lev2_logring;
  split_bin_2level(
    bins, bin,
    &lev1_ring, &lev1_logring, &lev2_ring, &lev2_logring,
    &new_lev1_ring, &new_lev1_logring, &new_lev2_ring, &new_lev2_logring
  );

  MPI_Finalize();

  return 0;
}
