#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <stdint.h>
#include "axl_internal.h"
#include "axl_async_bbapi.h"
#include <inttypes.h>
#include <pthread.h>
#include <limits.h>

/* These aren't always defined in linux/magic.h */

#ifdef HAVE_PTHREADS
#include "axl_pthread.h"
#endif

/* This is the IBM Burst Buffer transfer implementation.  The BB API only
 * supports transferring files between filesystems that support extents.  If
 * the user tries to transfer to an unsupported filesystem, we fallback to
 * a pthread transfer for the entire transfer if available. */

#include <bbapi.h>
#include <sys/statfs.h>
#include <linux/magic.h>
#include <libgen.h>
#include <unistd.h>

#ifndef GPFS_SUPER_MAGIC
#define GPFS_SUPER_MAGIC 0x47504653 /* "GPFS" in ASCII */
#endif
#ifndef XFS_SUPER_MAGIC
#define XFS_SUPER_MAGIC 0x58465342  /* "XFSB" in ASCII */
#endif

/* Global varibales */

uint64_t counter=0;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER; 

/* We're either running on a compute node with access to the BBAPI, or
 * we're running on a post-stage job node where we have to spawn bbapi.
 *
 * Return 1 if we're a post-stage node, 0 otherwise. */
static int this_is_a_post_stage_node(void)
{
    if (getenv("LSF_STAGE_USER_STAGE_OUT") != NULL) {
        return 1;
    }

    return 0;
}

/* Return the filesystem magic value for a given file */
static __fsword_t axl_get_fs_magic(char* path)
{
    /* Stat the path itself */
    struct statfs st;
    int rc = statfs(path, &st);
    if (rc != 0) {
        /* Couldn't statfs the path, which could be normal if the path is the
         * destination path.  Next, try stating the underlying path's directory
         * (which should exist for both the source and destination), for the
         * FS type. */
        char* dir = strdup(path);
        if (! dir) {
            return 0;
        }

        path = dirname(dir);

        rc = statfs(path, &st);
        if (rc != 0) {
            free(dir);
            return 0;
        }

        free(dir);
    }

    return st.f_type;
}

/* Returns 1 if its possible to transfer a file from the src to dst using the
 * BBAPI. In general (but not all cases) BBAPI can transfer between filesystems
 * if they both support extents.  EXT4 <-> gpfs is one exception.
 *
 * Returns 0 if BBAPI can not transfer from src to dst. */
int bbapi_copy_is_compatible(char* src, char* dst)
{
    /* List all filesystem types that are (somewhat) compatible with BBAPI */
    const __fsword_t whitelist[] = {
        XFS_SUPER_MAGIC,
        GPFS_SUPER_MAGIC,
        EXT4_SUPER_MAGIC,
    };

    __fsword_t source = axl_get_fs_magic(src);
    __fsword_t dest   = axl_get_fs_magic(dst);

    /* Exception: EXT4 <-> GPFS transfers are not supported by BBAPI */
    if ((source == EXT4_SUPER_MAGIC && dest == GPFS_SUPER_MAGIC) ||
        (source == GPFS_SUPER_MAGIC && dest == EXT4_SUPER_MAGIC))
    {
        return 0;
    }

    int found_source = 0;
    int found_dest   = 0;

    int i;
    for (i = 0; i < sizeof(whitelist) / sizeof(whitelist[0]); i++) {
        if (source == whitelist[i]) {
            found_source = 1;
        }

        if (dest == whitelist[i]) {
            found_dest = 1;
        }
    }

    if (found_source && found_dest) {
        return 1;
    }

    return 0;
}

static void getLastErrorDetails(BBERRORFORMAT pFormat, char** pBuffer)
{
    if (pBuffer) {
        size_t l_NumBytesAvailable;
        int rc = BB_GetLastErrorDetails(pFormat, &l_NumBytesAvailable, 0, NULL);
        if (rc == 0) {
            *pBuffer = (char*) malloc(l_NumBytesAvailable + 1);
            BB_GetLastErrorDetails(pFormat, NULL, l_NumBytesAvailable, *pBuffer);
        } else {
            *pBuffer = NULL;
        }
    }
}

/* Check and print BBAPI Error messages */
static int bb_check(int rc)
{
  if (rc) {
      char* errstring;
      getLastErrorDetails(BBERRORJSON, &errstring);
      AXL_ERR("AXL Error with BBAPI rc:       %d", rc);
      AXL_ERR("AXL Error with BBAPI details:  %s", errstring);
      free(errstring);

      //printf("Aborting due to failures\n");
      //exit(-1);
      return AXL_FAILURE;
  }
  return AXL_SUCCESS;
}

/* HACK
 *
 * Return a unique ID number for this node (tied to the hostname).
 *
 * The IBM BB API requires the user assign a unique node ID for the
 * 'contribid' when you start up the library.  IBM assumes you'd specify
 * the MPI rank here, but the bbapi, nor AXL, explicitly requires MPI.
 * Therefore, we return the numbers at the end of our hostname:
 * "sierra123" would return 123.
 *
 * This result is stored in id.  Returns 0 on success, nonzero otherwise. */
static int axl_get_unique_node_id(int* id)
{
    char hostname[256] = {0}; /* Max hostname + \0 */
    int rc = gethostname(hostname, sizeof(hostname));
    if (rc) {
        fprintf(stderr, "Hostname too long\n");
        return 1;
    }

    size_t len = strlen(hostname);

    rc = 1;

    /* Look from the back of the string to find the beginning of our number */
    int i;
    int sawnum = 0;
    for (i = len - 1; i >= 0; i--) {
        if (isdigit(hostname[i])) {
            sawnum = 1;
        } else {
            if (sawnum) {
                /*
                 * We were seeing a number, but now we've hit a non-digit.
                 * We're done.
                 */
                *id = atoi(&hostname[i + 1]);
                rc = 0;
                break;
            }
        }
    }
    return rc;
}

/* Called from AXL_Init */
int axl_async_init_bbapi(void)
{
    int rc;

    /* BBAPI uses the process MPI rank for its contributor id */
    int rank = axl_rank;
    if (rank == -1) {
        /* axl_rank was not set, create a rank value based on node id.
         * We also encode the thread id into the BBAPI tag, so it's valid
         * to have more than one process with the same rank here. */
        rc = axl_get_unique_node_id(&rank);
        if (rc) {
            AXL_ERR("Couldn't get unique node id");
            return AXL_FAILURE;
        }
    }

    rc = BB_InitLibrary(rank, BBAPI_CLIENTVERSIONSTR);
    if (rc && this_is_a_post_stage_node()) {
        /* We're running a post-stage node that can't connect to the BB Server
         * (which is to be expected).  Carry on. */
        return AXL_SUCCESS;
    } else {
        return bb_check(rc);
    }
}

/* Called from AXL_Finalize */
int axl_async_finalize_bbapi(void)
{
    if (!this_is_a_post_stage_node()) {
        int rc = BB_TerminateLibrary();
        return bb_check(rc);
    } else {
        return AXL_SUCCESS;
    }
}

/* Returns a unique BBTAG into *tag.  The tag returned must be unique
 * such that no two callers on the node will ever get the same tag
 * within a job.
 *
 * Returns 0 on success, 1 otherwise. */
static BBTAG axl_get_unique_tag(void)
{

  /* This is somewhat of a hack.
   *
   * We need a 64-bit tag that will never be repeated on this node.  To do
   * that we construct an ID of:
   *
   * 22 bit PID + 18 bit counter + 24 bit timestamp (33 years, to mitigate PID rollover)
   *
   */
  uint64_t counter_18;
  pthread_mutex_lock(&lock);
  counter_18 = counter & ((1ULL << 18) - 1); /* To capture call count and inter-thread calls */
  counter++;
  pthread_mutex_unlock(&lock);
  uint64_t pid_22 = getpid() &  ((1ULL << 22) - 1);
  uint64_t timestamp_24 = time(NULL) & ((1ULL << 24) - 1);
  uint64_t tag = (pid_22 << 42) | (counter_18 << 24) | timestamp_24;
  return tag;
}

/* Called from AXL_Create
 * BBTransferHandle and BBTransferDef are created and stored */
int axl_async_create_bbapi(int id)
{
    if (this_is_a_post_stage_node()) {
        return AXL_SUCCESS;
    }

    BBTransferDef_t* tdef;
    int rc = BB_CreateTransferDef(&tdef);
    if (rc) {
        /* If failed to create definition then return error. There is no cleanup necessary. */
        return bb_check(rc);
    }

    /* allocate a new transfer handle,
     * include AXL transfer id in IBM BB tag */
    BBTAG bbtag = axl_get_unique_tag();

    BBTransferHandle_t thandle;
    rc = BB_GetTransferHandle(bbtag, 1, NULL, &thandle);
    if (rc) {
        /* If transfer handle call failed then return. Do not record anything. */
        return bb_check(rc);
    }

    /* record transfer handle and definition */
    kvtree* file_list = axl_kvtrees[id];
    kvtree_util_set_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long) thandle);
    kvtree_util_set_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, tdef);

    return bb_check(rc);
}

/* Return the BBTransferHandle_t (which is just a uint64_t) for a given AXL id.
 *
 * You can only call this function after axl_async_create_bbapi(id) has been
 * called.
 *
 * Returns 0 on success, 1 on error.  On success, thandle contains the transfer
 * handle value. */
int axl_async_get_bbapi_handle(int id, uint64_t* thandle)
{
    kvtree* file_list = axl_kvtrees[id];
    if (kvtree_util_get_unsigned_long(file_list,
        AXL_BBAPI_KEY_TRANSFERHANDLE, thandle) != KVTREE_SUCCESS)
    {
        return AXL_FAILURE;
    }

    return AXL_SUCCESS;
}

/* Called from AXL_Add
 * Adds file source/destination to BBTransferDef */
int axl_async_add_bbapi (int id, const char* source, const char* dest)
{
    kvtree* file_list = axl_kvtrees[id];

    /* get transfer definition for this id */
    BBTransferDef_t* tdef = NULL;
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, (void**) &tdef);

    /* add file to transfer definition */
    int rc = BB_AddFiles(tdef, source, dest, 0);
    return bb_check(rc);
}

/* Called from AXL_Dispatch
 * Start the transfer, mark all files & set as INPROG
 * Assumes that mkdirs have already happened */
int __axl_async_start_bbapi (int id, int resume)
{
    kvtree* file_list = axl_kvtrees[id];

#ifdef HAVE_PTHREADS
    if (axl_bbapi_in_fallback(id)) {
        /* We're in fallback mode because some of the paths we want to
         * transfer from/to are not compatible with the BBAPI transfers (like
         * if the underlying filesystem doesn't support extent). */
        if (resume) {
            axl_pthread_resume(id);
        } else {
            axl_pthread_start(id);
        }
    }
#endif /* HAVE_PTHREADS */

    if (resume) {
        int old_status;
        kvtree_util_get_int(file_list, AXL_KEY_STATUS, &old_status);
        if (old_status == AXL_STATUS_INPROG) {
            /* Our transfers are already going.  Nothing to do */
            return AXL_SUCCESS;
        }
    }

    /* Pull BB-Def and BB-Handle out of global var */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);

    BBTransferDef_t* tdef;
    kvtree_util_get_ptr(file_list, AXL_BBAPI_KEY_TRANSFERDEF, (void**) &tdef);

#if 0
    /* TODO: is this necessary? */
    /* If there are 0 files, mark this as a success */
    int file_count = kvtree_size(files);
    if (file_count == 0) {
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_DEST);
        return AXL_SUCCESS;
    }
#endif

    /* Launch the transfer.  Note, while BB_StartTransfer() does launch an
     * asynchronous transfer, that doesn't mean it returns immediately.  For
     * example, when launched as a job, it can take the following amount of time
     * for the function to return:
     *
     * Amount   Startup time
     * ------   ------------
     * 1GB      1 sec
     * 10GB     5 sec
     * 20GB     10 sec
     * 100GB+   17 sec
     *
     * And for whatever reason, BB_StartTransfer() returns almost immediately
     * on interactive nodes.  A 20GB BB_StartTransfer() returns instantly, while
     * 100GB+ returns in 1-2 sec. */

    int rc = BB_StartTransfer(tdef, thandle);
    if (bb_check(rc) != AXL_SUCCESS) {
        /* something went wrong, update transfer to error state */
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_ERROR);
        rc = AXL_FAILURE;
        goto end;
    }

    /* mark this transfer as in progress */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);

    /* Mark all files as INPROG */
    kvtree_elem* elem;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_INPROG);
    }

    /* free the transfer definition */
    rc = BB_FreeTransferDef(tdef);
    bb_check(rc);

    /* drop transfer definition from kvtree */
    kvtree_unset(file_list, AXL_BBAPI_KEY_TRANSFERDEF);

    rc = AXL_SUCCESS;

end:
    axl_write_state_file(id);
    return rc;
}

int axl_async_start_bbapi (int id)
{
    return __axl_async_start_bbapi(id, 0);
}

int axl_async_resume_bbapi (int id)
{
    return __axl_async_start_bbapi(id, 1);
}

/* Check if a transfer is completed by spawning off 'bbcmd' to check the
 * transfer status.  This is the only way to check the transfer status
 * in a 2nd post-stage environment.
 *
 * Returns 1 if transfer status is BBFULLSUCCESS, 0 otherwise. */
static int transfer_is_complete_bbcmd(int id)
{
    kvtree* file_list = axl_kvtrees[id];

    /* Get the BB-Handle to query the status */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);

    /* Query all transfers that were BBFULLSUCCESS, and see if our handle
     * is one of them. */
    char* cmd = NULL;
    if (asprintf(&cmd,
        "/opt/ibm/bb/bin/bbcmd --pretty getstatus --target=0- --handle=%lu | grep -q BBFULLSUCCESS",
        (unsigned long) thandle) == -1)
    {
        AXL_ERR("Couldn't alloc memory for command\n");
        return 0;
    }

    int rc = system(cmd);
    free(cmd);
    if (rc != 0) {
        printf("Couldn't run command, rc = %d, errno = %d\n", rc, errno);
        return 0; /* failure */
    }

    return 1;
}

/* Check if a transfer is completed by using the BB API.  This can be used
 * from the compute node, but not the 2nd post-stage node.
 *
 * Returns 1 if transfer status is BBFULLSUCCESS, 0 otherwise.  */
static int transfer_is_complete_bbapi(int id)
{
    kvtree* file_list = axl_kvtrees[id];

    /* Get the BB-Handle to query the status */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);

    /* get info about transfer */
    BBTransferInfo_t tinfo;
    int rc = BB_GetTransferInfo(thandle, &tinfo);
    bb_check(rc);

    if (tinfo.status != BBFULLSUCCESS) {
        /* failure */
        return 0;
    }

    return 1;
}

int axl_async_test_bbapi (int id)
{
#ifdef HAVE_PTHREADS
    if (axl_bbapi_in_fallback(id)) {
        return axl_pthread_test(id);
    }
#endif /* HAVE_PTHREADS */

    /* We need to check to see if the transfers are done.  This is done
     * differently depending on if we're running on a compute node or on
     * the job node (in post-stage). */
    int rc;
    if (this_is_a_post_stage_node()) {
        /* We're on a job node (post-stage node) */
        rc = transfer_is_complete_bbcmd(id);
    } else {
        /* We're a compute node */
        rc = transfer_is_complete_bbapi(id);
    }

    /* check its status */
    int status;
    if (rc == 1) {
        /* Done transferring */
        status = AXL_STATUS_DEST;
    } else {
        status = AXL_STATUS_INPROG;
    }

    // TODO: add some finer-grain errror checking
    // BBSTATUS
    // - BBINPROGRESS
    // - BBCANCELED
    // - BBFAILED

    kvtree* file_list = axl_kvtrees[id];

    /* update status of set */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, status);

    /* update status of each file */
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, status);
    }

    /* test returns AXL_SUCCESS if AXL_Wait will not block */
    if (status == AXL_STATUS_DEST) {
        /* AXL_Wait will not block, so return success */
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_INPROG) {
        return AXL_FAILURE;
    } else if (status == AXL_STATUS_ERROR) {
        /* AXL_Wait will not block, so return success */
        return AXL_SUCCESS;
    }
}

int axl_async_wait_bbapi (int id)
{
    kvtree* file_list = axl_kvtrees[id];

#ifdef HAVE_PTHREADS
    if (axl_bbapi_in_fallback(id)) {
        return axl_pthread_wait(id);
    }
#endif /* HAVE_PTHREADS */

    /* Sleep until test changes set status */
    int status = AXL_STATUS_INPROG;
    while (status == AXL_STATUS_INPROG) {
        /* delegate work to test call to update status */
        axl_async_test_bbapi(id);

        /* if we're not done yet, sleep for some time and try again */
        kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);
        if (status == AXL_STATUS_INPROG) {
            if (this_is_a_post_stage_node()) {
                /* Use a long sleep since we may be spawning off 'bbcmd' to
                 * check the transfer status in axl_async_test_bbapi(), and
                 * it's slow. */
                sleep(2);
            } else {
                usleep(100 * 1000);   /* 100ms */
            }
        }
    }

    /* we're done now, either with error or success */
    if (status == AXL_STATUS_DEST) {
        /* Look though all our list of files */
        char* src;
        char* dst;
        kvtree_elem* elem = NULL;
        while ((elem = axl_get_next_path(id, elem, &src, &dst))) {
            AXL_DBG(2, "Read and copied %s to %s sucessfully", src, dst);
        }
        return AXL_SUCCESS;
    } else {
        return AXL_FAILURE;
    }
}

int axl_async_cancel_bbapi (int id)
{
#ifdef HAVE_PTHREADS
    if (axl_bbapi_in_fallback(id)) {
        return axl_pthread_cancel(id);
    }
#endif /* HAVE_PTHREADS */

    kvtree* file_list = axl_kvtrees[id];

    /* Get the BB-Handle to query the status */
    BBTransferHandle_t thandle;
    kvtree_util_get_unsigned_long(file_list, AXL_BBAPI_KEY_TRANSFERHANDLE, (unsigned long*) &thandle);

    /* TODO: want all processes to do this, or just one? */
    /* attempt to cancel this transfer */
    int rc = BB_CancelTransfer(thandle, BBSCOPETAG);
    int ret = bb_check(rc);

    /* TODO: can we update status of transfer at this point? */

    return ret;
}

/* Return 1 if all paths for a given id's filelist in the kvtree are compatible
 * with the BBAPI (all source and destination paths are on filesystems that
 * support extents).  Return 0 if any of the source or destination paths do not
 * support extents. */
int axl_all_paths_are_bbapi_compatible(int id)
{
    /* Look though all our list of files */
    char* src;
    char* dst;
    kvtree_elem* elem = NULL;
    while ((elem = axl_get_next_path(id, elem, &src, &dst))) {
        if (! bbapi_copy_is_compatible(src, dst)) {
            /* This file copy isn't compatible with BBAPI */
            return 0;
        }
    }

    /* All files copies are BBAPI compatible */
    return 1;
}

/* If the BBAPI is in fallback mode return 1, else return 0.
 *
 * Fallback mode happens when we can't transfer the files using the BBAPI due
 * to the source or destination nor supporting extents (which BBAPI requires).
 * If we're in fallback mode, we use a more compatible transfer method.
 *
 * Fallback mode is DISABLED by default.  You need to pass
 * -DENABLE_BBAPI_FALLBACK to cmake to enable it. */
int axl_bbapi_in_fallback(int id)
{
    int bbapi_fallback = 0;

#ifdef HAVE_BBAPI_FALLBACK
    kvtree* file_list = axl_kvtrees[id];
    if (kvtree_util_get_int(file_list, AXL_BBAPI_KEY_FALLBACK, &bbapi_fallback) !=
        KVTREE_SUCCESS)
    {
        /* Value isn't set, so we're not in fallback mode */
        return 0;
    }
#endif

    return bbapi_fallback;
}
