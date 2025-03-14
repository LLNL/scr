#ifndef REDSET_H
#define REDSET_H

#ifdef REDSET_ENABLE_MPI
#include "mpi.h"
#endif

/** \defgroup redset Redset
 *  \brief Redundancy encoding file sets
 *
 * Redset will create the redundancy data needed for a set of
 * files. It can rebuild a file with provided redundancy
 * information. */

/** \file redset.h
 *  \ingroup redset
 *  \brief redundancy sets */

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#define REDSET_SUCCESS (0)

#define REDSET_VERSION "0.4.0"

#define REDSET_COPY_NULL    (0)
#define REDSET_COPY_SINGLE  (1)
#define REDSET_COPY_PARTNER (2)
#define REDSET_COPY_XOR     (3)
#define REDSET_COPY_RS      (4)

/* names of user settable config parameters */
#define REDSET_KEY_CONFIG_SET_SIZE  "SETSIZE"
#define REDSET_KEY_CONFIG_MPI_BUF_SIZE "MPI_BUF_SIZE"
#define REDSET_KEY_CONFIG_DEBUG "DEBUG"

/********************************************************/
/** \name Define redundancy descriptor structure */
///@{
typedef void* redset;
typedef void* redset_filelist;
///@}

/********************************************************/
/** \name Redundancy descriptor functions */
///@{

/** initialize library */
int redset_init(void);

/** shutdown library */
int redset_finalize(void);

/* needs to be above doxygen comment to get association right */
typedef struct kvtree_struct kvtree;

/**
 * Get/set redset configuration values.
 *
 * The following configuration options can be set (type in parenthesis):
 *   * "DEBUG" (int) - if non-zero, output debug information from inside
 *     redset.
 *   * "SETSIZE" (int) - set size for redset to use.
 *   * "MPI_BUF_SIZE" (byte count [IN], int [OUT]) - MPI buffer size to chunk
 *     file transfer. Must not exceed INT_MAX.
 *   .
 * Symbolic names REDSET_KEY_CONFIG_FOO are defined in redset.h and should
 * be used instead of the strings whenever possible to guard against typos in
 * strings.
 *
 * \result If config != NULL, then return config on success.  If config == NULL
 *         (you're querying the config) then return a new kvtree on success,
 *         which must be kvtree_delete()ed by the caller. NULL on any failures.
 * \param config The new configuration. If config == NULL, then return a new
 *               kvtree with all the configuration values.
 *
 */
kvtree* redset_config(
  const kvtree *config /** < [IN] - options to be set */
);

#ifdef REDSET_ENABLE_MPI
/** create a new redundancy set descriptor */
int redset_create(
  int type,          /**< [IN]  - redundancy encoding type: one of REDSET_COPY values */
  MPI_Comm comm,     /**< [IN]  - process group participating in set */
  const char* group, /**< [IN]  - string specifying procs in the same failure group */
  redset* d          /**< [OUT] - output redundancy descriptor */
);

/** create a new redundancy set descriptor for SINGLE encoding */
int redset_create_single(
  MPI_Comm comm,     /**< [IN]  - process group participating in set */
  const char* group, /**< [IN]  - string specifying procs in the same failure group */
  redset* d          /**< [OUT] - output redundancy descriptor */
);

/** create a new redundancy set descriptor for PARTNER encoding */
int redset_create_partner(
  MPI_Comm comm,     /**< [IN]  - process group participating in set */
  const char* group, /**< [IN]  - string specifying procs in the same failure group */
  int size,          /**< [IN]  - minimum number of ranks for a redundancy set */
  int replicas,      /**< [IN]  - number of partner replicas */
  redset* d          /**< [OUT] - output redundancy descriptor */
);

/** create a new redundancy set descriptor for XOR encoding */
int redset_create_xor(
  MPI_Comm comm,     /**< [IN]  - process group participating in set */
  const char* group, /**< [IN]  - string specifying procs in the same failure group */
  int size,          /**< [IN]  - minimum number of ranks for a redundancy set */
  redset* d          /**< [OUT] - output redundancy descriptor */
);

/** create a new redundancy set descriptor for ReedSolomon encoding */
int redset_create_rs(
  MPI_Comm comm,     /**< [IN]  - process group participating in set */
  const char* group, /**< [IN]  - string specifying procs in the same failure group */
  int size,          /**< [IN]  - minimum number of ranks for a redundancy set */
  int k,             /**< [IN]  - number of encoding blocks [1,size) */
  redset* d          /**< [OUT] - output redundancy descriptor */
);
#endif

/** free any memory associated with the specified redundancy descriptor */
int redset_delete(
  redset* d /**< [INOUT] - redundancy descriptor to be freed */
);

/** apply redundancy scheme to file and return number of bytes copied
 * in bytes parameter */
int redset_apply(
  int numfiles,       /**< [IN] - number of file names in files array */
  const char** files, /**< [IN] - list of file names of length numfiles */
  const char* name,   /**< [IN] - path/filename prefix to prepend to redset metadata files */
  const redset d      /**< [IN] - redundancy decriptor to be applied */
);

#ifdef REDSET_ENABLE_MPI
/** rebuilds files for specified dataset id using specified redundancy descriptor,
 * adds them to filemap, and returns REDSET_SUCCESS if all processes succeeded */
int redset_recover(
  MPI_Comm comm,    /**< [IN]  - parent communicator containg set of processes rebuilding data */
  const char* name, /**< [IN]  - path/filename prefix to prepend to redset metadata files */
  redset* d         /**< [OUT] - output redundancy descriptor */
);
#endif

/** deletes redundancy data that was added in redset_apply,
 * which is useful when cleaning up */
int redset_unapply(
  const char* name, /**< [IN] - path/filename prefix to prepend to redset metadata files */
  const redset d    /**< [IN] - redundancy descriptor associated with above path */
);

/** return list of files added by redundancy scheme */
redset_filelist redset_filelist_enc_get(
  const char* name, /**< [IN] - path/filename prefix to prepend to redset metadata files */
  const redset d    /**< [IN] - redundancy descriptor associated with above path */
);

/** return list of original files encoded by redundancy scheme */
redset_filelist redset_filelist_orig_get(
  const char* name, /**< [IN] - path/filename prefix to prepend to redset metadata files */
  const redset d    /**< [IN] - redundancy descriptor associated with above path */
);

/** free file list allocated by call to redset_filelist_*_get */
int redset_filelist_release(
  redset_filelist* plist /**< [INOUT] - address of pointer to list to be freed, sets pointer to NULL */
);

/** return number of files in file list */
int redset_filelist_count(
  redset_filelist list /**< [IN] - list of redundancy files */
);

/** return name of file at specified index, where
 * 0 <= index < count and count is value returned by redset_filelist_count */
const char* redset_filelist_file(
  redset_filelist list, /**< [IN] - list of redundancy files */
  int index             /**< [IN] - index into list, ranges from 0 to list count - 1 */
);

/************************
 * The interfaces below are temporary.
 ***********************/

redset_filelist redset_filelist_get_data_partner(
  int num,
  const char** files,
  int* groupsize,
  int** groupranks
);

int redset_rebuild_partner(
  int num,
  const char** files,
  const char* prefix,
  const kvtree* map
);

redset_filelist redset_filelist_get_data_xor(
  int num,
  const char** files,
  int* groupsize,
  int** groupranks
);

int redset_rebuild_xor(
  int num,
  const char** files,
  const char* prefix,
  const kvtree* map
);

redset_filelist redset_filelist_get_data_rs(
  int num,
  const char** files,
  int* groupsize,
  int** groupranks
);

int redset_rebuild_rs(
  int num,
  const char** files,
  const char* prefix,
  const kvtree* map
);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* REDSET_H */
