#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"

#include <pthread.h>

#include "mpi.h"

#include "redset_io.h"
#include "redset_util.h"
#include "redset.h"
#include "redset_internal.h"
#include "redset_reedsolomon_common.h"

/* defines work for each thread along with data structures
 * to coordinate with main thread */
typedef struct thread_rs {
  int rank;           /* rank of thread (used for debugging) */
  const redset_reedsolomon* state; /* records pointer to struct defining GF */;
  unsigned char* a;   /* input/output buffer */
  unsigned char* b;   /* other input buffers */
  size_t n;           /* size of buffer in bytes */
  unsigned int coeff; /* GF coefficient by which to scale buffer b */
  unsigned char premult[256]; /* space to hold premult table */

  pthread_mutex_t mutex; /* mutex for condition vars below */

  int done;           /* worker sets to 1 when done */
  int main_waiting;   /* indicates main thread is waiting on cond_main */
  int thread_waiting; /* indicates worker thread is waiting on cond_thread */
  pthread_cond_t cond_main;   /* condition variable to wake main thread */
  pthread_cond_t cond_thread; /* condition variable to wake worker thread */

  struct thread_rs* next; /* for linked list */
} thread_rs_t;

typedef struct {
  pthread_mutex_t mutex; /* mutex for condition vars below */

  int main_waiting;         /* indicates main thread is waiting on cond_main */
  pthread_cond_t cond_main; /* condition variable to wake main thread */

  int thread_waiting;         /* counts number of worker threads waiting on cond_thread */
  pthread_cond_t cond_thread; /* condition variable to wake worker threads */

  int work_count;         /* number of work items ever defined (not just on work list) */
  thread_rs_t* work_head; /* list of work items, main process adds to list, workers pop */
  thread_rs_t* work_tail;

  int done_count;         /* running count of work items on done list */
  thread_rs_t* done_head; /* workers append to done list when work is complete */
  thread_rs_t* done_tail;
} thread_rs_queue_t;

/* data structure used on main process to track each thread it launches */
typedef struct {
  int num;           /* number of threads */
  pthread_t* tids;   /* pthread_t for each thread */
  thread_rs_t* data; /* data structure for each thread */
  thread_rs_queue_t* q;
} threadset_t;

#if 0
/* The actual pthread function */
static void* reduce_rs_pthread_fn2(void* arg)
{
  thread_rs_t* d = arg;
  while (1) {
    /* wait for work */
    pthread_mutex_lock(&d->mutex);
    while (d->done) {
      d->thread_waiting = 1;
      pthread_cond_wait(&d->cond_thread, &d->mutex);
      d->thread_waiting = 0;
    }
    pthread_mutex_unlock(&d->mutex);

    /* if signaled with 0 work, time to exit */
    if (d->n == 0) {
      break;
    }

    /* do work */
    redset_rs_reduce_buffer_multadd((redset_reedsolomon*)d->state, d->n, d->a, d->coeff, d->b);

    /* signal main thread that we're done */
    pthread_mutex_lock(&d->mutex);
    d->done = 1;
    if (d->main_waiting) {
      pthread_cond_signal(&d->cond_main);
    }
    pthread_mutex_unlock(&d->mutex);
  }
  pthread_exit(NULL);
}
#endif /* 0 */

static thread_rs_queue_t* queue_alloc(void)
{
  thread_rs_queue_t* q = (thread_rs_queue_t*) REDSET_MALLOC(sizeof(thread_rs_queue_t));

  pthread_mutex_init(&q->mutex, NULL);
  pthread_cond_init(&q->cond_thread, NULL);
  pthread_cond_init(&q->cond_main, NULL);

  q->main_waiting   = 0;
  q->thread_waiting = 0;

  q->work_count = 0;
  q->work_head = NULL;
  q->work_tail = NULL;

  q->done_count = 0;
  q->done_head = NULL;
  q->done_tail = NULL;

  return q;
}

static void queue_free(thread_rs_queue_t** qptr)
{
  thread_rs_queue_t* q = *qptr;

  /* delete list of completed work entries */
  thread_rs_t* head = q->done_head;
  while (head != NULL) {
    thread_rs_t* next = (thread_rs_t*)head->next;
    redset_free(&head);
    head = next;
  }

  /* shouldn't be anything left on the qork queue, but just in case */
  head = q->work_head;
  while (head != NULL) {
    thread_rs_t* next = (thread_rs_t*)head->next;
    redset_free(&head);
    head = next;
  }

  /* free condition variables and mutexes */
  pthread_cond_destroy(&q->cond_thread);
  pthread_cond_destroy(&q->cond_main);
  pthread_mutex_destroy(&q->mutex);

  /* free memory in data structure */
  redset_free(qptr);
}

/* pop work item off list,
 * requires we are holding the mutex */
static thread_rs_t* work_pop(thread_rs_queue_t* q)
{
  //assert(q->work_head != NULL);

  thread_rs_t* d = q->work_head;
  q->work_head = (thread_rs_t*)d->next;
  if (q->work_head == NULL) {
    q->work_tail = NULL;
  }

  d->next = NULL;
  return d;
}

/* append work descriptor to done queue,
 * requires we are holding the mutex */
static void done_append(thread_rs_queue_t* q, thread_rs_t* d)
{
  q->done_count += 1;
  if (q->done_head == NULL) {
    q->done_head = d;
  }
  if (q->done_tail != NULL) {
    q->done_tail->next = (struct thread_rs*)d;
  }
  q->done_tail = d;
}

/* pthread worker function */
static void* reduce_rs_pthread_fn3(void* arg)
{
  thread_rs_queue_t* q = arg;
  while (1) {
    /* wait for work */
    pthread_mutex_lock(&q->mutex);
    while (q->work_head == NULL) {
      q->thread_waiting += 1;
      pthread_cond_wait(&q->cond_thread, &q->mutex);
    }
    thread_rs_t* d = work_pop(q);
    pthread_mutex_unlock(&q->mutex);

    /* do work */
    if (d->n > 0) {
      size_t j;
      gf_premult_table(d->state, d->coeff, d->premult);
      for (j = 0; j < d->n; j++) {
        d->a[j] ^= d->premult[d->b[j]];
      }
      //redset_rs_reduce_buffer_multadd((redset_reedsolomon*)d->state, d->n, d->a, d->coeff, d->b);
    }

    /* if we have the last work item,
     * signal main thread that all work is done */
    pthread_mutex_lock(&q->mutex);
    done_append(q, d);
    if (q->main_waiting && q->done_count == q->work_count) {
      q->main_waiting = 0;
      pthread_cond_signal(&q->cond_main);
    }
    pthread_mutex_unlock(&q->mutex);

    /* if given item with 0 work,
     * that's our signal to join */
    if (d->n == 0) {
      break;
    }
  }
  pthread_exit(NULL);
}

#if 0
/* spawn a set of threads and fill in threadset structure to track them */
static int reduce_rs_pthread_setup(threadset_t* tset)
{
  int i;

  int ret = REDSET_SUCCESS;

  /* TODO: launch threads and attach to descriptor, activate via condition variable */

  /* compute number of threads to start up */
  int max_threads = 10;
  int nthreads = redset_get_nprocs();
  if (nthreads > max_threads) {
    nthreads = max_threads;
  }

  /* allocate pthread_t and data structure for each thread */
  pthread_t* tids   = (pthread_t*)   REDSET_MALLOC(nthreads * sizeof(pthread_t));
  thread_rs_t* data = (thread_rs_t*) REDSET_MALLOC(nthreads * sizeof(thread_rs_t));

  /* initialize mutex and condition variable for each thread */
  int rc;
  for (i = 0; i < nthreads; i++) {
    data[i].rank = i;
    data[i].done = 1;
    data[i].main_waiting = 0;
    data[i].thread_waiting = 0;
    rc = pthread_mutex_init(&data[i].mutex, NULL);
    rc = pthread_cond_init(&data[i].cond_thread, NULL);
    rc = pthread_cond_init(&data[i].cond_main, NULL);
  }

  /* start up each thread */
  for (i = 0; i < nthreads; i++) {
    rc = pthread_create(&tids[i], NULL, &reduce_rs_pthread_fn2, &data[i]);
    if (rc != 0) {
      ret = REDSET_FAILURE;
    }
  }

  tset->num  = nthreads;
  tset->tids = tids;
  tset->data = data;
  tset->q    = NULL;

  return ret;
}

/* given data, assign work to threads and perform XOR reduction */
static int reduce_rs_pthread_execute(
  threadset_t* tset,
  const redset_reedsolomon* state,
  size_t count,
  unsigned char* a,
  unsigned int coeff,
  unsigned char* b)
{
  int i;

  int ret = REDSET_SUCCESS;

  /* TODO: launch threads and attach to descriptor, activate via condition variable */

  int nthreads = tset->num;

  /* compute number of bytes for each thread to reduce */
  size_t size = count / nthreads;
  if (size * nthreads < count) {
    size += 1;
  }

  /* TODO: ensure size of some minimum */

  /* define work descriptor for each thread */
  size_t offset = 0;
  for (i = 0; i < nthreads; i++) {
    size_t amt = count - offset;
    if (amt > size) {
      amt = size;
    }

    thread_rs_t* data = &tset->data[i];
    data->state = state;
    data->a     = a + offset;
    data->b     = b + offset;
    data->n     = amt;
    data->coeff = coeff;

    offset += amt;
  }

  /* signal each worker that it has something to do */
  for (i = 0; i < nthreads; i++) {
    if (tset->data[i].n > 0) {
      pthread_mutex_lock(&tset->data[i].mutex);
      tset->data[i].done = 0;
      if (tset->data[i].thread_waiting) {
        pthread_cond_signal(&tset->data[i].cond_thread);
      }
      pthread_mutex_unlock(&tset->data[i].mutex);
    }
  }

  /* wait for each worker to signal that it is done */
  for (i = 0; i < nthreads; i++) {
    if (tset->data[i].n > 0) {
      pthread_mutex_lock(&tset->data[i].mutex);
      while (! tset->data[i].done) {
        tset->data[i].main_waiting = 1;
        pthread_cond_wait(&tset->data[i].cond_main, &tset->data[i].mutex);
        tset->data[i].main_waiting = 0;
      }
      pthread_mutex_unlock(&tset->data[i].mutex);
    }
  }

  return ret;
}
#endif /* 0 */

#if 0
/* signal threads to shutdown, wait for them to exit, and free resources in thread set */
static int reduce_rs_pthread_teardown(threadset_t* tset)
{
  int i;

  int ret = REDSET_SUCCESS;

  /* signal each thread with 0 work to indicate it should exit */
  for (i = 0; i < tset->num; i++) {
    pthread_mutex_lock(&tset->data[i].mutex);
    tset->data[i].n = 0;
    tset->data[i].done = 0;
    if (tset->data[i].thread_waiting) {
      pthread_cond_signal(&tset->data[i].cond_thread);
    }
    pthread_mutex_unlock(&tset->data[i].mutex);
  }

  /* wait for all threads to exit */
  for (i = 0; i < tset->num; i++) {
    void* retval;
    int rc = pthread_join(tset->tids[i], &retval);
    if (rc != 0) {
      ret = REDSET_FAILURE;
    }
  }

  /* free condition variables and mutexes */
  for (i = 0; i < tset->num; i++) {
    pthread_cond_destroy(&tset->data[i].cond_thread);
    pthread_cond_destroy(&tset->data[i].cond_main);
    pthread_mutex_destroy(&tset->data[i].mutex);
  }

  /* free memory in data structure */
  redset_free(&tset->data);
  redset_free(&tset->tids);
  tset->num = 0;

  return ret;
}
#endif /* 0 */

/* spawn a set of threads and fill in threadset structure to track them */
static int reduce_rs_pthread_setup3(threadset_t* tset)
{
  int ret = REDSET_SUCCESS;

  /* compute number of threads to start up */
  int max_threads = 10;
  int nthreads = redset_get_nprocs();
  if (nthreads > max_threads) {
    nthreads = max_threads;
  }

  /* allocate pthread_t and data structure for each thread */
  pthread_t* tids = (pthread_t*) REDSET_MALLOC(nthreads * sizeof(pthread_t));

  thread_rs_queue_t* q = queue_alloc();

  /* start up each thread */
  int i;
  for (i = 0; i < nthreads; i++) {
    int rc = pthread_create(&tids[i], NULL, &reduce_rs_pthread_fn3, q);
    if (rc != 0) {
      ret = REDSET_FAILURE;
    }
  }

  tset->num  = nthreads;
  tset->tids = tids;
  tset->data = NULL;
  tset->q    = q;

  return ret;
}

static void append_work_queue(threadset_t* tset, int count, thread_rs_t* head, thread_rs_t* tail)
{
  thread_rs_queue_t* q = tset->q;

  /* signal each worker that it has something to do */
  pthread_mutex_lock(&q->mutex);

  q->work_count += count;

  /* append new entries to work queue */
  if (q->work_head == NULL) {
    q->work_head = head;
  } else {
    q->work_tail->next = (struct thread_rs*)head;
  }
  q->work_tail = tail;

  /* signal threads to wake up and check queue */
  if (q->thread_waiting > 0) {
    q->thread_waiting = 0;
    pthread_cond_broadcast(&q->cond_thread);
  }
  pthread_mutex_unlock(&q->mutex);
}

/* given data, assign work to threads and perform XOR reduction */
static int reduce_rs_pthread_launch3(
  threadset_t* tset,
  const redset_reedsolomon* state,
  size_t count,
  unsigned char* a,
  unsigned int coeff,
  unsigned char* b)
{
  int ret = REDSET_SUCCESS;

  int nthreads = tset->num;

  /* compute number of bytes for each thread to reduce */
  size_t size = count / nthreads;
  if (size * nthreads < count) {
    size += 1;
  }

  /* TODO: ensure size of some minimum */

  /* define work descriptor for each thread */
  int i;
  thread_rs_t* head = NULL;
  thread_rs_t* tail = NULL;
  int work_count = 0;
  size_t offset = 0;
  for (i = 0; i < nthreads; i++) {
    size_t amt = count - offset;
    if (amt > size) {
      amt = size;
    }

    if (amt > 0) {
      thread_rs_t* d = (thread_rs_t*) REDSET_MALLOC(sizeof(thread_rs_t));
      d->state = state;
      d->a     = a + offset;
      d->b     = b + offset;
      d->n     = amt;
      d->coeff = coeff;
      d->next  = NULL;

      if (head == NULL) {
        head = d;
      } else {
        tail->next = (struct thread_rs*)d;
      }
      tail = d;

      work_count += 1;
    }

    offset += amt;
  }

  append_work_queue(tset, work_count, head, tail);

  return ret;
}

/* given data, assign work to threads and perform XOR reduction */
static void reduce_rs_pthread_sync3(threadset_t* tset)
{
  /* wait for all work to be done */
  thread_rs_queue_t* q = tset->q;
  pthread_mutex_lock(&q->mutex);
  while (q->done_count < q->work_count) {
    q->main_waiting = 1;
    pthread_cond_wait(&q->cond_main, &q->mutex);
  }
  q->done_count = 0;
  q->work_count = 0;
  pthread_mutex_unlock(&q->mutex);
}

/* signal threads to shutdown, wait for them to exit, and free resources in thread set */
static int reduce_rs_pthread_teardown3(threadset_t* tset)
{
  int i;

  int ret = REDSET_SUCCESS;

  /* signal each thread with 0 work to indicate it should exit */
  thread_rs_t* head = NULL;
  thread_rs_t* tail = NULL;
  int work_count = 0;
  for (i = 0; i < tset->num; i++) {
    thread_rs_t* d = (thread_rs_t*) REDSET_MALLOC(sizeof(thread_rs_t));
    d->n    = 0;
    d->next = NULL;

    if (head == NULL) {
      head = d;
    } else {
      tail->next = (struct thread_rs*)d;
    }
    tail = d;

    work_count += 1;
  }

  append_work_queue(tset, work_count, head, tail);

  /* wait for all threads to exit */
  for (i = 0; i < tset->num; i++) {
    void* retval;
    int rc = pthread_join(tset->tids[i], &retval);
    if (rc != 0) {
      ret = REDSET_FAILURE;
    }
  }

  /* free memory in data structure */
  queue_free(&tset->q);
  redset_free(&tset->tids);
  tset->num = 0;

  return ret;
}

/* apply ReedSolomon redundancy scheme to dataset files */
int redset_reedsolomon_encode_pthreads(
  const redset_base* d,
  redset_lofi rsf,
  const char* chunk_file,
  int fd_chunk,
  size_t chunk_size)
{
  int rc = REDSET_SUCCESS;
  int i;

  /* get pointer to RS state structure */
  redset_reedsolomon* state = (redset_reedsolomon*) d->state;

  /* get offset into file immediately following the header */
  off_t header_size = lseek(fd_chunk, 0, SEEK_CUR);

  /* allocate buffers to hold reduction result, send buffer, and receive buffer */
  unsigned char** data_bufs = (unsigned char**) redset_buffers_alloc(state->encoding, redset_mpi_buf_size);
  unsigned char** recv_bufs = (unsigned char**) redset_buffers_alloc(state->encoding, redset_mpi_buf_size);

  /* allocate buffer to read a piece of my file */
  unsigned char** send_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* send_buf = send_bufs[0];

  threadset_t threads;
#if 0
  reduce_rs_pthread_setup(&threads);
#else
  reduce_rs_pthread_setup3(&threads);
#endif /* 0 */

  /* we'll issue a send/recv for each encoding block in each step */
  MPI_Request* request = (MPI_Request*) REDSET_MALLOC(state->encoding * 2 * sizeof(MPI_Request));
  MPI_Status*  status  = (MPI_Status*)  REDSET_MALLOC(state->encoding * 2 * sizeof(MPI_Status));

  /* process all data for this chunk */
  size_t nread = 0;
  while (nread < chunk_size) {
    /* limit the amount of data we read from the file at a time */
    size_t count = chunk_size - nread;
    if (count > redset_mpi_buf_size) {
      count = redset_mpi_buf_size;
    }

    /* initialize our reduction buffers */
    for (i = 0; i < state->encoding; i++) {
      memset(data_bufs[i], 0, count);
    }

    /* In each step below, we read a chunk from our data files,
     * and send that data to the k ranks responsible for encoding
     * the checksums.  In each step, we'll receive a sliver of
     * data for each of the k blocks this process is responsible
     * for encoding */
    int chunk_step;
    for (chunk_step = d->ranks - 1; chunk_step >= state->encoding; chunk_step--) {
      /* get the chunk id for the current chunk */
      int chunk_id = (d->rank + chunk_step) % d->ranks;

      /* compute offset to read from within our file */
      int chunk_id_rel = redset_rs_get_data_id(d->ranks, state->encoding, d->rank, chunk_id);
      unsigned long offset = chunk_size * (unsigned long) chunk_id_rel + nread;

      /* read data from our file into send buffer */
      if (redset_lofi_pread(&rsf, send_buf, count, offset) != REDSET_SUCCESS)
      {
        /* read failed, make sure we fail this rebuild */
        rc = REDSET_FAILURE;
      }

      /* send data from our file to k ranks, and receive
       * incoming data from k ranks */
      int k = 0;
      for (i = 0; i < state->encoding; i++) {
        /* distance we're sending or receiving in this round */
        int dist = d->ranks - chunk_step + i;

        /* receive data from the right */
        int rhs_rank = (d->rank + dist + d->ranks) % d->ranks;
        MPI_Irecv(recv_bufs[i], count, MPI_BYTE, rhs_rank, 0, d->comm, &request[k]);
        k++;

        /* send our data to the left */
        int lhs_rank = (d->rank - dist + d->ranks) % d->ranks;
        MPI_Isend(send_buf, count, MPI_BYTE, lhs_rank, 0, d->comm, &request[k]);
        k++;
      }

      /* wait for communication to complete */
      MPI_Waitall(k, request, status);

      /* encode received data into our reduction buffers */
      for (i = 0; i < state->encoding; i++) {
        /* compute rank that sent to us */
        int dist = d->ranks - chunk_step + i;
        int received_rank = (d->rank + dist + d->ranks) % d->ranks;

        /* encode received data using its corresponding matrix
         * coefficient and accumulate to our reductino buffer */
        int row = i + d->ranks;
        unsigned int coeff = state->mat[row * d->ranks + received_rank];
#if 0
        reduce_rs_pthread_execute(&threads, state, count, data_bufs[i], coeff, recv_bufs[i]);
#else
        reduce_rs_pthread_launch3(&threads, state, count, data_bufs[i], coeff, recv_bufs[i]);
#endif /* 0 */
      }

      /* wait for threads to finish */
      reduce_rs_pthread_sync3(&threads);
    }

    /* write final encoded data to our chunk file */
    for (i = 0; i < state->encoding; i++) {
      off_t offset = i * chunk_size + nread + header_size;
      if (redset_lseek(chunk_file, fd_chunk, offset, SEEK_SET) != REDSET_SUCCESS) {
        rc = REDSET_FAILURE;
      }
      if (redset_write_attempt(chunk_file, fd_chunk, data_bufs[i], count) != count) {
        rc = REDSET_FAILURE;
      }
    }

    nread += count;
  }

  redset_free(&request);
  redset_free(&status);

  /* free buffers */
  redset_buffers_free(state->encoding, &data_bufs);
  redset_buffers_free(state->encoding, &recv_bufs);
  redset_buffers_free(1,               &send_bufs);

#if 0
  reduce_rs_pthread_teardown(&threads);
#else
  reduce_rs_pthread_teardown3(&threads);
#endif /* 0 */

  return rc;
}
