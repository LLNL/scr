#include "axl_internal.h"
#include "kvtree_util.h"

#include "axl_sync.h"

#include <assert.h>

/* synchonous transfer of files */
int __axl_sync_start (int id, int resume)
{
    /* assume we'll succeed */
    int rc = AXL_SUCCESS;

    /* get pointer to file list for this dataset */
    kvtree* file_list = axl_kvtrees[id];

    /* mark dataset as in progress */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);

    /* transfer each of my files and fill in summary data structure */
    kvtree_elem* elem = NULL;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        /* get the filename */
        char* source = kvtree_elem_key(elem);

        /* get the hash for this file */
        kvtree* elem_hash = kvtree_elem_hash(elem);

        /* WEIRD case: we've restarted a sync transfer that was going */
        int status;
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);
        if (status == AXL_STATUS_DEST) {
            /* this file was already transfered */
            continue;
        }

        /* TODO: check bytecount conversion success, do not use global
         * axl_kvtrees to get file_list */
        unsigned long file_buf_size;
        int success = kvtree_util_get_bytecount(file_list,
            AXL_KEY_CONFIG_FILE_BUF_SIZE, &file_buf_size);
        assert(success == KVTREE_SUCCESS);

        char* destination;
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, &destination);

        /* Copy the file */
        int tmp_rc = axl_file_copy(source, destination, file_buf_size, resume);
        if (tmp_rc == AXL_SUCCESS) {
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_DEST);
        } else {
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_ERROR);
            rc = AXL_FAILURE;
        }
        AXL_DBG(2, "axl_sync_start: Read and copied %s to %s with success code %d",
            source, destination, tmp_rc
        );
    }

    /* mark this id as either done or error */
    if (rc == AXL_SUCCESS) {
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_DEST);
    } else {
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_ERROR);
    }

    return rc;
}

int axl_sync_start (int id)
{
    return __axl_sync_start(id, 0);
}

int axl_sync_resume(int id)
{
    return __axl_sync_start(id, 1);
}

int axl_sync_test (int id)
{
    /* since everything completed during start,
     * we're done */
    return AXL_SUCCESS;
}

int axl_sync_wait (int id)
{
    /* get pointer to file list for this dataset */
    kvtree* file_list = axl_kvtrees[id];

    /* determine whether transfer was successful */
    int status;
    if (kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status) == KVTREE_SUCCESS) {
        if (status == AXL_STATUS_DEST) {
            /* explicitly marked as done */
            return AXL_SUCCESS;
        }
    }

    /* otherwise, assume we failed */
    return AXL_FAILURE;
}
