#include "kvtree.h"
#include "axl_internal.h"

// TODO: technically not needed
#include "axl_async_datawarp.h"

#include "datawarp.h"

int axl_async_start_datawarp(int id)
{
    /* Record that we started transfer of this file list */
    kvtree* file_list = kvtree_get_kv_int(axl_async_file_lists, AXL_KEY_HANDLE_UID, id);
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);
 
    /* iterate over files */
    kvtree_elem* elem;
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        /* get the filename */
        char* file = kvtree_elem_key(elem);
 
        /* get the hash for this file */
        kvtree* elem_hash = kvtree_elem_hash(elem);
 
        /* get the destination for this file */
        char* dest_file;
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, &dest_file);
 
        /* Stage out file */
        int stage_out = dw_stage_file_out(file, dest_file, DW_STAGE_IMMEDIATE);
        if (stage_out != 0) {
            axl_abort(-1,
                "Datawarp stage file out failed with error %d @ %s:%d",
                -stage_out, __FILE__, __LINE__
            );
        }
 
        /* record that the file is in progress */
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_INPROG);
    }
 
    return AXL_SUCCESS;
}

int axl_async_complete_datawarp(int id)
{
    /* no datawarp specific code for async complete */
    return AXL_SUCCESS;
}

int axl_async_stop_datawarp(int id)
{
    kvtree* file_list = kvtree_get_kv_int(axl_async_file_lists, AXL_KEY_HANDLE_UID, id);
    kvtree* files     = kvtree_get(file_list, AXL_KEY_FILES);

    /* iterate over files */
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        char* file = kvtree_elem_key(elem);

        dw_terminate_file_stage(file);

        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_SOURCE);
    }

    /* mark full file list as not transfered */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_SOURCE);

    return AXL_SUCCESS;
}

int axl_async_test_datawarp(int id)
{
    /* assume transfer is complete */
    int transfer_complete = 1;

    int complete = 0;
    int pending  = 0;
    int deferred = 0;
    int failed   = 0;

    kvtree* file_list = kvtree_get_kv_int(axl_async_file_lists, AXL_KEY_HANDLE_UID, id);
    kvtree* files     = kvtree_get(file_list, AXL_KEY_FILES);

    /* iterate over files */
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        kvtree* elem_hash = kvtree_elem_hash(elem);

        int status;
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);

        switch (status) {
            case AXL_STATUS_DEST:
                break;
            case AXL_STATUS_INPROG:
                char* file = kvtree_elem_key(elem);

                int test_complete;
                int test_pending;
                int test_deferred;
                int test_failed;
                int test = dw_query_file_stage(
                    file,
                    &test_complete,
                    &test_pending,
                    &test_deferred,
                    &test_failed
                );

                if (test == 0) {
                   complete += test_complete;
                   pending  += test_pending;
                   deferred += test_deferred;
                   failed   += test_failed;

                   int file_status = AXL_STATUS_INPROG;
                   if (test_failed) {
                      file_status = AXL_STATUS_ERROR;
                   } else if (test_complete) {
                      file_status = AXL_STATUS_DEST;
                   }
                   kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, file_status);
                } else {
                   axl_abort(-1, "Datawarp failed with error %d @ %s:%d", -test, __FILE__, __LINE__);
                }
                break;
            case AXL_STATUS_ERROR:
            case AXL_STATUS_SOURCE:
            default:
                axl_abort(-1, "Wait called on file with invalid status @ %s:%d", __FILE__, __LINE__);
        }
    }

    int status = AXL_STATUS_DEST;
    if (failed != 0) {
       status = AXL_STATUS_ERROR;
    } else if (pending != 0 || deferred != 0) {
       transfer_complete = 0;
       status = AXL_STATUS_INPROG
    }

    /* record the transfer status */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, status);

    if (transfer_complete) {
       return AXL_SUCCESS
    }
}

int axl_async_wait_datawarp(int id)
{
    /* Get the list of files */
    int dw_wait       = 0;

    kvtree* file_list = kvtree_get_kv_int(axl_async_file_lists, AXL_KEY_HANDLE_UID, id);
    kvtree* files     = kvtree_get(file_list, AXL_KEY_FILES);

    /* iterate over files */
    kvtree_elem* elem = NULL;
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        kvtree* elem_hash = kvtree_elem_hash(elem);

        int status;
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);

        switch (status) {
            case AXL_STATUS_DEST:
                break;
            case AXL_STATUS_INPROG:
                /* Do the wait on each file */
                char* file = kvtree_elem_key(elem);
                dw_wait = dw_wait_file_stage(file);
                if (dw_wait != 0) {
                    axl_abort(-1,
                        "Datawarp wait operation failed with error %d @ %s:%d",
                        -dw_wait, __FILE__, __LINE__
                    );
                }
                kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_DEST);
                break;
            case AXL_STATUS_SOURCE:
            case AXL_STATUS_ERROR:
            default:
                axl_abort(-1,
                    "Wait operation called on file with invalid status @ %s:%d",
                    __FILE__, __LINE__
                );
        }
    }

    /* record transfer complete */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_DEST);
    return AXL_SUCCESS;
}

int axl_async_init_datawarp()
{
    return AXL_SUCCESS;
}

int axl_async_finalize_datawarp()
{
    return AXL_SUCCESS;
}
