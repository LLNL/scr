#include <stdlib.h>
#include <string.h>
#include <libgen.h>

#include "axl.h"
#include "axl_mpi.h"

#include "kvtree.h"
#include "kvtree_util.h"

#include "config.h"

#include "mpi.h"

#define AXL_FAILURE (1)

static int axl_alltrue(int valid, MPI_Comm comm)
{
    int all_valid;
    MPI_Allreduce(&valid, &all_valid, 1, MPI_INT, MPI_LAND, comm);
    return all_valid;
}

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void axl_free2(void* p) {
    /* verify that we got a valid pointer to a pointer */
    if (p != NULL) {
        /* free memory if there is any */
        void* ptr = *(void**)p;
        if (ptr != NULL) {
            free(ptr);
        }

        /* set caller's pointer to NULL */
        *(void**)p = NULL;
    }
}

int AXL_Init_comm (
    MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    /* initialize AXL */
    int rc = AXL_Init();

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */

        /* if our call to init succeeded,
         * cal finalize to clean up */
        if (rc == AXL_SUCCESS) {
            AXL_Finalize();
        }

        /* return failure to everyone */
        return AXL_FAILURE;
    }

    return rc;
}

int AXL_Finalize_comm (
    MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_SUCCESS;

    int axl_rc = AXL_Finalize();
    if (axl_rc != AXL_SUCCESS) {
        rc = axl_rc;
    }

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */

        /* return failure to everyone */
        rc = AXL_FAILURE;
    }

    return rc;
}

#include <stdio.h>

int AXL_Create_comm (
    axl_xfer_t type,  /**< [IN]  - AXL transfer type (AXL_XFER_SYNC, AXL_XFER_PTHREAD, etc) */
    const char* name,
    const char* file,
    MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    int id = AXL_Create(type, name, file);

    /* NOTE: We do not force id to be the same on all ranks.
     * It may be useful to do that, but then we need collective
     * allocation. */

    /* return same value on all ranks */
    if (! axl_alltrue(id != -1, comm)) {
      /* someone failed, so everyone fails */

      /* if this process succeeded in create,
       * free its handle to clean up */
      if (id != -1) {
          AXL_Free(id);
      }

      /* return -1 to everyone */
      id = -1;
    }

    return id;
}

int AXL_Add_comm (
  int id,           /**< [IN]  - transfer hander ID returned from AXL_Create */
  int num,          /**< [IN]  - number of files in src and dst file lists */
  const char** src, /**< [IN]  - list of source paths of length num */
  const char** dst, /**< [IN]  - list of destination paths of length num */
  MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    /* assume we'll succeed */
    int rc = AXL_SUCCESS;

    /* add files to transfer list */
    int i;
    for (i = 0; i < num; i++) {
      const char* src_file  = src[i];
      const char* dest_file = dst[i];
      if (AXL_Add(id, src_file, dest_file) != AXL_SUCCESS) {
        /* remember that we failed to add a file */
        rc = AXL_FAILURE;
      }
    }

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Dispatch_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    /* delegate remaining work to regular dispatch */
    int rc = AXL_Dispatch(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */

        /* TODO: do we have another option than cancel/wait? */
        /* If dispatch succeeded on this process, cancel and wait.
         * This is ugly but necessary since the caller will free
         * the handle when we return, since we're telling the caller
         * that the collective dispatch failed.  The handle needs
         * to be in a state that can be freed. */
        if (rc == AXL_SUCCESS) {
            AXL_Cancel(id);
            AXL_Wait(id);

            /* TODO: should we also delete files,
             * since they may have already been transferred? */
        }

        /* return failure to everyone */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Test_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Test(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Wait_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Wait(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Cancel_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Cancel(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Free_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Free(id);

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}

int AXL_Stop_comm (
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_Stop();

    /* return same value on all ranks */
    if (! axl_alltrue(rc == AXL_SUCCESS, comm)) {
        /* someone failed, so everyone fails */
        rc = AXL_FAILURE;
    }

    return rc;
}
