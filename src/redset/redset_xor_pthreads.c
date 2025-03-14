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

/* defines work for each thread along with data structures
 * to coordinate with main thread */
typedef struct {
  int rank;         /* rank of thread (used for debugging) */
  unsigned char* a; /* input/output buffer */
  unsigned char* b; /* other input buffer */
  size_t n;         /* size of buffer in bytes */

  pthread_mutex_t mutex; /* mutex for condition vars below */

  int done;           /* worker sets to 1 when done */
  int main_waiting;   /* indicates main thread is waiting on cond_main */
  int thread_waiting; /* indicates worker thread is waiting on cond_thread */
  pthread_cond_t cond_main;   /* condition variable to wake main thread */
  pthread_cond_t cond_thread; /* condition variable to wake worker thread */
} thread_xor_t;

/* data structure used on main process to track each thread it launches */
typedef struct {
  int num;            /* number of threads */
  pthread_t* tids;    /* pthread_t for each thread */
  thread_xor_t* data; /* data structure for each thread */
} threadset_t;

/* XOR a with b, store result in a */
static void reduce_xor(unsigned char* a, const unsigned char* b, size_t count)
{
  size_t i;
  for (i = 0; i < count; i++) {
    a[i] ^= b[i];
  }
}

/* The actual pthread function */
static void* reduce_xor_pthread_fn2(void* arg)
{
  thread_xor_t* d = arg;
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
    reduce_xor(d->a, d->b, d->n);

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

/* spawn a set of threads and fill in threadset structure to track them */
static int reduce_xor_pthread_setup(threadset_t* tset)
{
  int i;

  int ret = REDSET_SUCCESS;

  /* TODO: launch threads and attach to descriptor, activate via condition variable */

  /* compute number of threads to start up */
  int max_threads = 16;
  int nthreads = redset_get_nprocs();
  if (nthreads > max_threads) {
    nthreads = max_threads;
  }

  /* allocate pthread_t and data structure for each thread */
  pthread_t* tids    = (pthread_t*)    REDSET_MALLOC(nthreads * sizeof(pthread_t));
  thread_xor_t* data = (thread_xor_t*) REDSET_MALLOC(nthreads * sizeof(thread_xor_t));

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
    rc = pthread_create(&tids[i], NULL, &reduce_xor_pthread_fn2, &data[i]);
    if (rc != 0) {
      ret = REDSET_FAILURE;
    }
  }

  tset->num  = nthreads;
  tset->tids = tids;
  tset->data = data;

  return ret;
}

/* given data, assign work to threads and perform XOR reduction */
static int reduce_xor_pthread_execute(threadset_t* tset, unsigned char* a, unsigned char* b, size_t count)
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

    tset->data[i].a = a + offset;
    tset->data[i].b = b + offset;
    tset->data[i].n = amt;

    offset += amt;
  }

  /* signal each worker that has something to do */
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

/* signal threads to shutdown, wait for them to exit, and free resources in thread set */
static int reduce_xor_pthread_teardown(threadset_t* tset)
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

int redset_xor_encode_pthreads(
  const redset_base* d,
  redset_lofi rsf,
  const char* my_chunk_file,
  int fd_chunk,
  size_t chunk_size)
{
  int rc = REDSET_SUCCESS;

  /* get pointer to XOR state structure */
  redset_xor* state = (redset_xor*) d->state;

  threadset_t threads;
  reduce_xor_pthread_setup(&threads);

  /* allocate buffer to read a piece of my file */
  unsigned char** send_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* send_buf = send_bufs[0];

  /* allocate buffer to read a piece of the recevied chunk file */
  unsigned char** recv_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* recv_buf = recv_bufs[0];

  MPI_Request request[2];
  MPI_Status  status[2];

  /* XOR Reduce_scatter */
  size_t nread = 0;
  while (nread < chunk_size) {
    size_t count = chunk_size - nread;
    if (count > redset_mpi_buf_size) {
      count = redset_mpi_buf_size;
    }

    int chunk_id;
    for(chunk_id = d->ranks-1; chunk_id >= 0; chunk_id--) {
      /* read the next set of bytes for this chunk from my file into send_buf */
      if (chunk_id > 0) {
        int chunk_id_rel = (d->rank + d->ranks + chunk_id) % d->ranks;
        if (chunk_id_rel > d->rank) {
          chunk_id_rel--;
        }
        unsigned long offset = chunk_size * (unsigned long) chunk_id_rel + nread;
        if (redset_lofi_pread(&rsf, send_buf, count, offset) != REDSET_SUCCESS)
        {
          rc = REDSET_FAILURE;
        }
      } else {
        memset(send_buf, 0, count);
      }

      /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
      /* merge the blocks via xor operation */
      if (chunk_id < d->ranks-1) {
        reduce_xor_pthread_execute(&threads, send_buf, recv_buf, count);
      }

      if (chunk_id > 0) {
        /* not our chunk to write, forward it on and get the next */
        MPI_Irecv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &request[0]);
        MPI_Isend(send_buf, count, MPI_BYTE, state->rhs_rank, 0, d->comm, &request[1]);
        MPI_Waitall(2, request, status);
      } else {
        /* write send block to send chunk file */
        if (redset_write_attempt(my_chunk_file, fd_chunk, send_buf, count) != count) {
          rc = REDSET_FAILURE;
        }
      }
    }

    nread += count;
  }

  /* free the buffers */
  redset_buffers_free(1, &send_bufs);
  redset_buffers_free(1, &recv_bufs);

  reduce_xor_pthread_teardown(&threads);

  return rc;
}

int redset_xor_decode_pthreads(
  const redset_base* d,
  int root,
  redset_lofi rsf,
  const char* chunk_file,
  int fd_chunk,
  size_t chunk_size)
{
  int rc = REDSET_SUCCESS;

  /* get pointer to XOR state structure */
  redset_xor* state = (redset_xor*) d->state;

  /* spawn up a thread pool */
  threadset_t threads;
  reduce_xor_pthread_setup(&threads);

  /* allocate buffer to read a piece of my file */
  unsigned char** send_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* send_buf = send_bufs[0];

  /* allocate buffer to read a piece of the recevied chunk file */
  unsigned char** recv_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* recv_buf = recv_bufs[0];

  /* Pipelined XOR Reduce to root */
  MPI_Status status[2];
  unsigned long offset = 0;
  int chunk_id;
  for (chunk_id = 0; chunk_id < d->ranks; chunk_id++) {
    size_t nread = 0;
    while (nread < chunk_size) {
      size_t count = chunk_size - nread;
      if (count > redset_mpi_buf_size) {
        count = redset_mpi_buf_size;
      }

      if (root != d->rank) {
        /* read the next set of bytes for this chunk from my file into send_buf */
        if (chunk_id != d->rank) {
          /* for this chunk, read data from the logical file */
          if (redset_lofi_pread(&rsf, send_buf, count, offset) != REDSET_SUCCESS)
          {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, read data from the XOR file */
          if (redset_read_attempt(chunk_file, fd_chunk, send_buf, count) != count) {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        }

        /* if not start of pipeline, receive data from left and xor with my own */
        if (root != state->lhs_rank) {
          MPI_Recv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &status[0]);

          reduce_xor_pthread_execute(&threads, send_buf, recv_buf, count);
        }

        /* send data to right-side partner */
        MPI_Send(send_buf, count, MPI_BYTE, state->rhs_rank, 0, d->comm);
      } else {
        /* root of rebuild, just receive incoming chunks and write them out */
        MPI_Recv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &status[0]);

        /* if this is not my xor chunk, write data to normal file, otherwise write to my xor chunk */
        if (chunk_id != d->rank) {
          /* for this chunk, write data to the logical file */
          if (redset_lofi_pwrite(&rsf, recv_buf, count, offset) != REDSET_SUCCESS)
          {
            /* write failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, write data from the XOR file */
          if (redset_write_attempt(chunk_file, fd_chunk, recv_buf, count) != count) {
            /* write failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        }
      }

      nread += count;
    }
  }

  /* free the buffers */
  redset_buffers_free(1, &recv_bufs);
  redset_buffers_free(1, &send_bufs);

  /* shut down the threads */
  reduce_xor_pthread_teardown(&threads);

  return rc;
}
