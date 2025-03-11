#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "mpi.h"

#include "redset_io.h"
#include "redset_util.h"
#include "redset.h"
#include "redset_internal.h"

static __global__ void xor_gpu(unsigned char* a, unsigned char* b, int n)
{
  size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < n) {
    a[i] ^= b[i];
  }
}

/* apply XOR redundancy scheme to dataset files */
int redset_xor_encode_gpu(
  const redset_base* d,
  redset_lofi rsf,
  const char* chunk_file,
  int fd_chunk,
  size_t chunk_size)
{
  int rc = REDSET_SUCCESS;

 /* get pointer to XOR state structure */
  redset_xor* state = (redset_xor*) d->state;

  /* allocate buffer to read a piece of my file */
  unsigned char** host_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* host_buf = host_bufs[0];

  unsigned char* send_buf;
  unsigned char* recv_buf;
  cudaMalloc(&send_buf, redset_mpi_buf_size);
  cudaMalloc(&recv_buf, redset_mpi_buf_size);

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
        if (redset_lofi_pread(&rsf, host_buf, count, offset) != REDSET_SUCCESS)
        {
          rc = REDSET_FAILURE;
        }
      } else {
        memset(host_buf, 0, count);
      }

      /* copy file data from host to device */
      cudaMemcpy(send_buf, host_buf, count, cudaMemcpyHostToDevice);

      /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
      /* merge the blocks via xor operation */
      if (chunk_id < d->ranks-1) {
        int nthreads = 1024;
        int nblocks = (count + nthreads - 1) / nthreads;
        xor_gpu<<<nblocks, nthreads>>>(send_buf, recv_buf, count);
        cudaDeviceSynchronize();
      }

      if (chunk_id > 0) {
        /* not our chunk to write, forward it on and get the next */
        MPI_Irecv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &request[0]);
        MPI_Isend(send_buf, count, MPI_BYTE, state->rhs_rank, 0, d->comm, &request[1]);
        MPI_Waitall(2, request, status);
      } else {
        /* copy data from device to host for writing */
        cudaMemcpy(host_buf, send_buf, count, cudaMemcpyDeviceToHost);

        /* write send block to send chunk file */
        if (redset_write_attempt(chunk_file, fd_chunk, host_buf, count) != count) {
          rc = REDSET_FAILURE;
        }
      }
    }

    nread += count;
  }

  cudaFree(recv_buf);
  cudaFree(send_buf);
  recv_buf = NULL;
  send_buf = NULL;

  /* free the buffers */
  redset_buffers_free(1, &host_bufs);

  return rc;
}

/* decode XOR redundancy scheme to rebuild missing files */
int redset_xor_decode_gpu(
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

  /* allocate buffer to read a piece of my file */
  unsigned char** host_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* host_buf = host_bufs[0];

  unsigned char* send_buf;
  unsigned char* recv_buf;
  cudaMalloc(&send_buf, redset_mpi_buf_size);
  cudaMalloc(&recv_buf, redset_mpi_buf_size);

  MPI_Status status[2];

  /* Pipelined XOR Reduce to root */
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
        /* read the next set of bytes for this chunk from my file into host_buf */
        if (chunk_id != d->rank) {
          /* for this chunk, read data from the logical file */
          if (redset_lofi_pread(&rsf, host_buf, count, offset) != REDSET_SUCCESS)
          {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, read data from the XOR file */
          if (redset_read_attempt(chunk_file, fd_chunk, host_buf, count) != count) {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        }

        /* copy file data from host to device */
        cudaMemcpy(send_buf, host_buf, count, cudaMemcpyHostToDevice);

        /* if not start of pipeline, receive data from left and xor with my own */
        if (root != state->lhs_rank) {
          MPI_Recv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &status[0]);

          int nthreads = 1024;
          int nblocks = (count + nthreads - 1) / nthreads;
          xor_gpu<<<nblocks, nthreads>>>(send_buf, recv_buf, count);
          cudaDeviceSynchronize();
        }

        /* send data to right-side partner */
        MPI_Send(send_buf, count, MPI_BYTE, state->rhs_rank, 0, d->comm);
      } else {
        /* TODO: skip memcpy by receive direct to host_buf */
        /* root of rebuild, just receive incoming chunks and write them out */
        MPI_Recv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &status[0]);

        /* copy data from device to host for writing */
        cudaMemcpy(host_buf, recv_buf, count, cudaMemcpyDeviceToHost);

        /* if this is not my xor chunk, write data to normal file, otherwise write to my xor chunk */
        if (chunk_id != d->rank) {
          /* for this chunk, write data to the logical file */
          if (redset_lofi_pwrite(&rsf, host_buf, count, offset) != REDSET_SUCCESS)
          {
            /* write failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, write data from the XOR file */
          if (redset_write_attempt(chunk_file, fd_chunk, host_buf, count) != count) {
            /* write failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        }
      }

      nread += count;
    }
  }

  cudaFree(recv_buf);
  cudaFree(send_buf);
  recv_buf = NULL;
  send_buf = NULL;

  /* free the buffers */
  redset_buffers_free(1, &host_bufs);

  return rc;
}

#ifdef __cplusplus
} /* extern C */
#endif
