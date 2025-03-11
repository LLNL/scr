#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <assert.h>
#include "axl_internal.h"
#include "kvtree_util.h"
#include "axl_pthread.h"
#include "axl_sync.h"

/* get_nprocs() */
#if defined(__APPLE__)
#include <sys/sysctl.h>
#else
#include <sys/sysinfo.h>
#endif

#define AXL_MIN(a,b) (a < b ? a : b)

/*  We default our number of threads to the lesser of:
 *
 *  - The number of CPU threads
 *  - MAX_THREADS
 *  - The number of files being transferred */

/* We don't see much scaling past 16 threads */
#define MAX_THREADS 16

struct axl_work
{
    struct axl_work* next;

    /* The file element in the kvtree */
    kvtree_elem* elem;
};

struct axl_pthread_data
{
    /* AXL ID associated with this data */
    int id;

    /* AXL transfer options from axl_kvtrees */
    kvtree* file_list;

    /* If resume = 1, try to resume old transfers */
    int resume;

    /* Number of threads */
    unsigned int threads;

    /* This struct is in a linked list */
    struct axl_pthread_data* next;

    /* Lock to protect workqueue */
    pthread_mutex_t lock;

    /* Our workqueue */
    struct axl_work* head;
    struct axl_work* tail;

    /* Tracks count of work items still to be completed.
     * The main thread increments this with each item it adds to the queue.
     * Each thread decrements this counter when it completes an item.
     * After starting the threads, the main thread tests whether
     * this count has reached 0 to know that all work is done. */
    unsigned int remain;

    /* Array of our thread IDs */
    pthread_t* tid;
};

/* This is a linked list that is used to lookup which axl_pthread_data is
 * associated with each ID.  Why put it in a linked list instead of just
 * storing the pdata in the kvtree?  Because if we put it in a kvtree,
 * and the app dies, the pdata pointer becomes stale, and would get
 * erroneously freed as part of an AXL_Stop().  Instead we use
 * axl_pthread_data_lookup(), axl_pthread_data_add(), and
 * axl_pthread_data_remove() to access the data.  This makes it so the pdata
 * is ephemeral, only existing while the app is running. */
struct axl_all_pthread_data
{
    struct axl_pthread_data* head;
    struct axl_pthread_data* tail;

    /* Lock to protect this data structure */
    pthread_mutex_t lock;
} axl_all_pthread_data = {
    .head = NULL,
    .tail = NULL,
    .lock = PTHREAD_MUTEX_INITIALIZER
};

/* Linux and OSX compatible 'get number of hardware threads' */
unsigned int axl_get_nprocs(void)
{
    unsigned int cpu_threads;

#if defined(__APPLE__)
    int count;
    size_t size = sizeof(count);
    if (sysctlbyname("hw.ncpu", &count, &size, NULL, 0)) {
        cpu_threads = 1;
    } else {
        cpu_threads = count;
    }
#else
    cpu_threads = get_nprocs();
#endif

    return cpu_threads;
}

/* Get the pthread data for a given AXL ID */
struct axl_pthread_data* axl_pthread_data_lookup(int id)
{
    struct axl_pthread_data* ret = NULL;

    pthread_mutex_lock(&axl_all_pthread_data.lock);

    struct axl_pthread_data* pdata = axl_all_pthread_data.head;
    while (pdata) {
        if (pdata->id == id) {
            /* Match */
            ret = pdata;
            break;
        }
        pdata = pdata->next;
    }

    pthread_mutex_unlock(&axl_all_pthread_data.lock);

    return ret;
}

/* Add our new pdata to the list. */
void axl_pthread_data_add(int id, struct axl_pthread_data* pdata)
{
    pdata->id = id;
    pdata->file_list = axl_kvtrees[id];

    pthread_mutex_lock(&axl_all_pthread_data.lock);

    if (!axl_all_pthread_data.head) {
        /* First entry */
        axl_all_pthread_data.head = pdata;
        axl_all_pthread_data.tail = pdata;
    } else {
       axl_all_pthread_data.tail->next = pdata;
       axl_all_pthread_data.tail = pdata;
    }

    pthread_mutex_unlock(&axl_all_pthread_data.lock);
}

void axl_pthread_data_remove(int id)
{
    pthread_mutex_lock(&axl_all_pthread_data.lock);

    struct axl_pthread_data* prev = NULL;
    struct axl_pthread_data* pdata = axl_all_pthread_data.head;
    while (pdata) {
         if (pdata->id == id) {
            /* Match, remove it from the list.  The user is still responsible
             * for freeing pdata with axl_pthread_free_pdata(). */
            if (prev) {
                prev->next = pdata->next;
            }
            if (axl_all_pthread_data.head == pdata) {
                axl_all_pthread_data.head = pdata->next;
            }
            if (axl_all_pthread_data.tail == pdata) {
                axl_all_pthread_data.tail = prev;
            }
            break;
        }
        prev = pdata;
        pdata = pdata->next;
    }

    pthread_mutex_unlock(&axl_all_pthread_data.lock);
}

/* The actual pthread function */
static void* axl_pthread_func(void* arg)
{
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    struct axl_pthread_data* pdata = arg;
    while (1) {
        pthread_mutex_lock(&pdata->lock);

        /* Get a file to transfer */
        struct axl_work* work = pdata->head;
        if (! work) {
            /* No more work to do */
            pthread_mutex_unlock(&pdata->lock);
            break;
        } else {
            /* Take our work out of the queue */
            pdata->head = pdata->head->next;
            pthread_mutex_unlock(&pdata->lock);
        }

        /* Get kvtree for this file */
        kvtree_elem* elem = work->elem;
        kvtree* elem_hash = kvtree_elem_hash(elem);

        /* Get source file name */
        char* src = kvtree_elem_key(elem);

        /* Lookup destination filename */
        char* dst = NULL;
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, &dst);

        const kvtree* file_list = pdata->file_list;

        unsigned long file_buf_size;
        int success = kvtree_util_get_bytecount(file_list,
            AXL_KEY_CONFIG_FILE_BUF_SIZE, &file_buf_size);
        assert(success == KVTREE_SUCCESS);

        /* Copy the file from soruce to destination */
        int rc = axl_file_copy(src, dst, file_buf_size, pdata->resume);
        AXL_DBG(2, "%s: Read and copied %s to %s, rc %d",
            __func__, src, dst, rc);

        /* Record the success/failure of the individual file transfer */
        if (rc == AXL_SUCCESS) {
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_DEST);
        } else {
            kvtree_util_set_int(elem_hash, AXL_KEY_FILE_STATUS, AXL_STATUS_ERROR);
        }

        free(work);

        /* record that one more work item is done (though perhaps with an error) */
        pthread_mutex_lock(&pdata->lock);
        pdata->remain -= 1;
        pthread_mutex_unlock(&pdata->lock);
    }

    return AXL_SUCCESS;
}

static struct axl_pthread_data* axl_pthread_create_thread_data(unsigned int threads)
{
    struct axl_pthread_data* pdata = calloc(1, sizeof(*pdata));
    if (! pdata) {
        return NULL;
    }

    if (pthread_mutex_init(&pdata->lock, NULL) != 0) {
        free(pdata);
        return NULL;
    }

    pdata->tid = calloc(threads, sizeof(pdata->tid[0]));
    if (! pdata->tid) {
        free(pdata);
        return NULL;
    }

    pdata->threads = threads;
    pdata->head    = NULL;
    pdata->tail    = NULL;
    pdata->remain  = 0;

    return pdata;
}

/* Free up our axl_pthread_data.  We assume no one is competing for the lock. */
static void axl_pthread_free_pdata(struct axl_pthread_data* pdata)
{
    /* Free our workqueue item first (if any).  If our transfer completed
     * successfully, all workqueue entries would have already been freed. */
    struct axl_work* work = pdata->head;
    while (pdata->head) {
        work = pdata->head->next;
        free(pdata->head);
        pdata->head = work;
    }

    free(pdata->tid);
    free(pdata);
}

/* Add a file to our workqueue.  We assume the lock is already held */
static int axl_pthread_add_work(struct axl_pthread_data* pdata, kvtree_elem* elem)
{
    struct axl_work* work = calloc(1, sizeof(*work));
    if (! work) {
        return AXL_FAILURE;
    }

    work->elem = elem;

    if (! pdata->head) {
         /* First time we inserted into the workqueue */
        pdata->head = work;
        pdata->tail = work;
    } else {
        /* Insert it at the end of the list */
        pdata->tail->next = work;
        pdata->tail = work;
    }

    /* Increment our count of items to be completed */
    pdata->remain += 1;

    return AXL_SUCCESS;
}

/* Create and wakeup all our threads to start transferring the files in
 * the workqueue. */
static int axl_pthread_run(struct axl_pthread_data* pdata)
{
    int i;
    for (i = 0; i < pdata->threads; i++) {
        int rc = pthread_create(&pdata->tid[i], NULL, &axl_pthread_func, pdata);
        if (rc != 0) {
            return AXL_FAILURE;
        }
    }

    return AXL_SUCCESS;
}

/* Start a tranfer.  If resume = 1, attempt to resume the old transfer (start
 * the copy where the old destination file left off). */
static int __axl_pthread_start (int id, int resume)
{
    /* assume we'll succeed */
    int rc = AXL_SUCCESS;

    /* get pointer to file list for this dataset */
    kvtree* file_list = axl_kvtrees[id];

    /* mark dataset as in progress */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);

    /* Get number of files being transferred */
    kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
    unsigned int file_count = kvtree_size(files);

    /* Get number of hardware threads */
    unsigned int cpu_threads = axl_get_nprocs();

    unsigned int threads = AXL_MIN(cpu_threads, AXL_MIN(file_count, MAX_THREADS));

    /* Create the data structure for our threads */
    struct axl_pthread_data* pdata = axl_pthread_create_thread_data(threads);
    if (! pdata) {
        return AXL_FAILURE;
    }
    pdata->resume = resume;

    axl_pthread_data_add(id, pdata);

    kvtree_elem* elem = NULL;
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        /* get the hash for this file */
        kvtree* elem_hash = kvtree_elem_hash(elem);

        int status;
        kvtree_util_get_int(elem_hash, AXL_KEY_FILE_STATUS, &status);
        if (status == AXL_STATUS_DEST) {
            /* this file was already transfered */
            continue;
        }

        rc = axl_pthread_add_work(pdata, elem);
        if (rc != AXL_SUCCESS) {
            printf("something bad happened\n");
            /* Something bad happened.  Break here instead of returning so
             * that pdata gets freed (at the end of this function). */
            break;
        }
    }

    /* At this point, all our files are queued in the workqueue.  Start the
     * transfers. */
    rc = axl_pthread_run(pdata);
    if (rc == AXL_SUCCESS) {
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_INPROG);
    } else {
        AXL_ERR("Couldn't spawn all threads");
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_ERROR);
    }

    return rc;
}

int axl_pthread_start (int id)
{
    return __axl_pthread_start (id, 0);
}

int axl_pthread_resume (int id)
{
    return __axl_pthread_start (id, 1);
}

int axl_pthread_test (int id)
{
    /* assume all items are done */
    int rc = AXL_SUCCESS;

    struct axl_pthread_data* pdata = axl_pthread_data_lookup(id);
    assert(pdata);

    /* check whether all work items have been completed */
    pthread_mutex_lock(&pdata->lock);
    if (pdata->remain > 0) {
        /* There is still work outstanding */
        rc = AXL_FAILURE;
    }
    pthread_mutex_unlock(&pdata->lock);

    return rc;
}

int axl_pthread_wait (int id)
{
    int rc = AXL_SUCCESS;

    struct axl_pthread_data* pdata = axl_pthread_data_lookup(id);
    if (! pdata) {
        /* Did they call AXL_Cancel() and then AXL_Wait()? */
        AXL_ERR("No pthread data");
        return AXL_FAILURE;
    }

    /* get pointer to file list for this dataset */
    kvtree* file_list = pdata->file_list;

    /* All our threads are now started.  Wait for them to finish */
    int i;
    for (i = 0; i < pdata->threads; i++) {
        void* rc_ptr = NULL;
        int tmp_rc = pthread_join(pdata->tid[i], &rc_ptr);
        if (tmp_rc != 0) {
            AXL_ERR("pthread_join(%d) failed (%d)", i, rc);
            return AXL_FAILURE;
        }

        /* Check the rc that the thread actually reported.  The thread
         * returns a void * that we encode our rc value in.  If the
         * thread was canceled, that totally valid and fine. */
        if (rc_ptr != PTHREAD_CANCELED) {
            tmp_rc = (int) ((unsigned long) rc_ptr);
            if (tmp_rc) {
                AXL_ERR("pthread join rc_ptr was set as %d", rc);
                rc = AXL_FAILURE;
            }
        }
    }

    if (rc != AXL_SUCCESS) {
        AXL_ERR("Couldn't join all threads");
        return AXL_FAILURE;
    }

    axl_pthread_data_remove(id);
    axl_pthread_free_pdata(pdata);

    /* All threads are now successfully finished */
    kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_DEST);

    return axl_sync_wait(id);
}

int axl_pthread_cancel (int id)
{
    int rc = AXL_SUCCESS;

    /* get pointer to pthread struct for this dataset */
    struct axl_pthread_data* pdata = axl_pthread_data_lookup(id);
    assert(pdata);

    int i;
    for (i = 0; i < pdata->threads; i++) {
        /* send the thread a cancellation request */
        int tmp_rc = pthread_cancel(pdata->tid[i]);
        if (tmp_rc) {
            AXL_ERR("pthread_cancel failed, rc %d");
            rc = AXL_FAILURE;
            break;
        }

        /* wait for the thread to actually exit */
        void* rc_ptr;
        pthread_join(pdata->tid[i], &rc_ptr);
        if (rc_ptr != 0 && rc_ptr != PTHREAD_CANCELED) {
            AXL_ERR("pthread_join failed, rc_ptr %p", rc_ptr);
            rc = AXL_FAILURE;
        }
    }

    if (rc != AXL_SUCCESS) {
        AXL_ERR("Bad return code from canceling a thread");
    }

    return rc;
}

void axl_pthread_free (int id)
{
    /* pdata should have been freed in AXL_Wait(), but maybe they just did
     * an AXL_Cancel() and then an AXL_Free().  If so, pdata will be set
     * and we should free it. */
    struct axl_pthread_data* pdata = axl_pthread_data_lookup(id);
    if (pdata) {
        axl_pthread_data_remove(id);
        axl_pthread_free_pdata(pdata);
    }
}
