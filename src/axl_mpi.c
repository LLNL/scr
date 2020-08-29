#include <stdlib.h>
#include <string.h>
#include <libgen.h>

#include "axl.h"
#include "axl_mpi.h"

#include "kvtree.h"
#include "kvtree_util.h"

#include "config.h"

#include "mpi.h"

#include "scr_globals.h"
#define AXL_FAILURE (1)
//#define HAVE_LIBDTCMP 1

#ifdef HAVE_LIBDTCMP
#include "dtcmp.h"
#endif

static int axl_alltrue(int valid, MPI_Comm comm)
{
    int all_valid;
    MPI_Allreduce(&valid, &all_valid, 1, MPI_INT, MPI_LAND, comm);
    return all_valid;
}

/* allocate size bytes, returns NULL if size == 0,
 * calls er_abort if allocation fails */
static void* axl_malloc(size_t size, const char* file, int line)
{
    void* ptr = NULL;
    if (size > 0) {
        ptr = malloc(size);
        if (ptr == NULL) {
            scr_abort(-1, "Failed to allocate %llu bytes @ %s:%d",
                (unsigned long long) size, file, line
            );
        }
    }
    return ptr;
}
#define AXL_MALLOC(X) axl_malloc(X, __FILE__, __LINE__);

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

/* build list of directories needed for file list (one per file) */
static int axl_create_dirs(int count, const char** filelist, MPI_Comm comm)
{
    /* TODO: need to list dirs in order from parent to child */

    /* allocate buffers to hold the directory needed for each file */
    int* leader           = (int*)         AXL_MALLOC(sizeof(int)         * count);
    const char** dirs     = (const char**) AXL_MALLOC(sizeof(const char*) * count);
    uint64_t* group_id    = (uint64_t*)    AXL_MALLOC(sizeof(uint64_t)    * count);
    uint64_t* group_ranks = (uint64_t*)    AXL_MALLOC(sizeof(uint64_t)    * count);
    uint64_t* group_rank  = (uint64_t*)    AXL_MALLOC(sizeof(uint64_t)    * count);

    /* lookup directory from meta data for each file */
    int i;
    for (i = 0; i < count; i++) {
        /* extract directory from filename */
        const char* filename = filelist[i];
        char* path = strdup(filename);
        dirs[i] = strdup(dirname(path));
        axl_free2(&path);

        /* lookup original path where application wants file to go */
#ifdef HAVE_LIBDTCMP
        /* we'll use DTCMP to select one leader for each directory later */
        leader[i] = 0;
#else
        /* if we don't have DTCMP,
         * then we'll just issue a mkdir for each file, lots of extra
         * load on the file system, but this works */
        leader[i] = 1;
#endif
    }

#ifdef HAVE_LIBDTCMP
    /* with DTCMP we identify a single process to create each directory */

    /* identify the set of unique directories */
    uint64_t groups;
    DTCMP_Rankv_strings(
        count, dirs, &groups, group_id, group_ranks, group_rank,
        DTCMP_FLAG_NONE, comm
    );

    /* select leader for each directory */
    for (i = 0; i < count; i++) {
        if (group_rank[i] == 0) {
            leader[i] = 1;
        }
    }
#endif /* HAVE_LIBDTCMP */

    /* get file mode for directory permissions */
    mode_t mode_dir = axl_getmode(1, 1, 1);

    /* TODO: add flow control here */

    /* create other directories in file list */
    int success = 1;
    for (i = 0; i < count; i++) {
        /* get dirname */
        const char* dir = dirs[i];

        /* if we're the leader, create directory */
        if (leader[i]) {
            if (axl_mkdir(dir, mode_dir) != AXL_SUCCESS) {
                success = 0;
            }
        }

        /* free the dirname we strdup'd */
        axl_free2(&dir);
    }

    /* free buffers */
    axl_free2(&group_id);
    axl_free2(&group_ranks);
    axl_free2(&group_rank);
    axl_free2(&dirs);
    axl_free2(&leader);

    /* determine whether all leaders successfully created their directories */
    if (! axl_alltrue(success == 1, comm)) {
        return AXL_FAILURE;
    }

    return AXL_SUCCESS;
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

#ifdef HAVE_LIBDTCMP
    int dtcmp_rc = DTCMP_Init();
    if (dtcmp_rc != DTCMP_SUCCESS) {
        /* failed to initialize DTCMP */
        rc = AXL_FAILURE;
    }
#endif

    return rc;
}

int AXL_Finalize_comm (
    MPI_Comm comm)    /**< [IN]  - communicator used for coordination and flow control */
{
    int rc = AXL_SUCCESS;

#ifdef HAVE_LIBDTCMP
    int dtcmp_rc = DTCMP_Finalize();
    if (dtcmp_rc != DTCMP_SUCCESS) {
        /* failed to shut down DTCMP */
        rc = AXL_FAILURE;
    }
#endif

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

int AXL_Dispatch_comm (
    int id,        /**< [IN]  - transfer hander ID returned from AXL_Create */
    MPI_Comm comm) /**< [IN]  - communicator used for coordination and flow control */
{
#if 0
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to dispatch */
    if (xstate != AXL_XFER_STATE_CREATED) {
        AXL_ERR("Invalid state to dispatch UID %d", id);
        return AXL_FAILURE;
    }
    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_DISPATCHED);
#endif

#if 0
    /* create destination directories for each file */
    if (axl_make_directories) {
        /* count number of files we have */
        kvtree* file_list = kvtree_get_kv_int(axl_file_lists, AXL_KEY_HANDLE_UID, id);
        kvtree* files_hash = kvtree_get(file_list, AXL_KEY_FILES);
        int num_files = kvtree_size(files_hash);

        /* allocate pointer for each one */
        const char** files = (const char**) AXL_MALLOC(num_files * sizeof(char*));

        /* set pointer to each file */
        int i;
        char* dest;
        kvtree_elem* elem;
        while ((elem = axl_get_next_path(id, elem, NULL, &dest))) {
            files[i] = dest;
            i++;
        }

        /* create directories */
        axl_create_dirs(num_files, files, comm);

        /* free list of files */
        axl_free2(&files);
    }

    /* TODO: this is hacky */
    /* delegate remaining work to regular dispatch,
     * but disable mkdir since we already did that */
    int make_dir = axl_make_directories;
    axl_make_directories = 0;
    int rc = AXL_Dispatch(id);
    axl_make_directories = make_dir;
#endif
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
