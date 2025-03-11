#ifndef RANKSTR_MPI_H
#define RANKSTR_MPI_H

/** \defgroup rankstr Rankstr
 *  \brief Splits processes into different MPI communicators given a string
 *
 * Rankstr uses bitonic sort for a scalable method to identify process
 * groups. It is useful to create a communicator of ranks that all
 * share the same storage device, then rank 0 in this communicator can
 * create directory and inform others that dir has been created with
 * barrier. It is also used to split processes into groups based on
 * failure group (failure group of NODE --> splits MPI_COMM_WORLD into
 * subgroups based on hostname). */

/** \file rankstr_mpi.h
 *  \ingroup rankstr
 *  \brief Split mpi ranks into different communicators given a string */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#define RANKSTR_VERSION "0.4.0"

/** Given a communicator and a string, compute number of unique strings
 * across all procs in comm and compute an id for input string
 * such that the id value matches another process if and only if that
 * process specified an identical string. The groupid value will range
 * from 0 to groups-1. */
void rankstr_mpi(
  const char* str, /**< [IN]  - input string (pointer) */
  MPI_Comm comm,   /**< [IN]  - communicator of processes (handle) */
  int tag1,        /**< [IN]  - tag to use for point-to-point communication on comm */
  int tag2,        /**< [IN]  - another tag, distinct from tag1, for point-to-point on comm */
  int* groups,     /**< [OUT] - number of unique strings (non-negative integer) */
  int* groupid     /**< [OUT] - id for input string (non-negative integer) */
);

/** split input comm into sub communicators, each of which contains
 * all procs specifying the same value of str, and reordered according
 * to key, ranks providing str == NULL will return newcomm == MPI_COMM_NULL */
void rankstr_mpi_comm_split(
  MPI_Comm comm,    /**< [IN]  - communicator of processes (handle) */
  const char* str,  /**< [IN]  - input string (pointer) */
  int key,          /**< [IN]  - key to order ranks in new communicator */
  int tag1,         /**< [IN]  - tag to use for point-to-point communication on comm */
  int tag2,         /**< [IN]  - another tag, distinct from tag1, for point-to-point on comm */
  MPI_Comm* newcomm /**< [OUT] - output communicator */
);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
