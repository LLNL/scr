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
#include "redset_reedsolomon_common.h"

static __global__ void add_gpu(unsigned char* a, unsigned char* b, int n)
{
  size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < n) {
    a[i] ^= b[i];
  }
}

static __global__ void multadd_gpu(unsigned int* gf_log, unsigned int* gf_exp, int gf_size, size_t count, unsigned char* dbuf, unsigned int coeff, unsigned char* rbuf)
{
  /* TODO: read gf_log into gf_exp thread-shared memory */

  size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < count && coeff != 0) {
    /* 0 times anything is 0, we treat this as a special case since
     * there is no entry for 0 in the log table below, since there
     * is no value of x such that 2^x = 0 */
    int data = rbuf[i];
    if (data != 0) {
      /* compute (v1 * v2) product as 2^( log_2(v1) + log_2(v2) ) in GF(2^bits) arithmetic */
      int sumlogs = gf_log[coeff] + gf_log[data];
      if (sumlogs >= gf_size - 1) {
        sumlogs -= (gf_size - 1);
      }
      dbuf[i] ^= (unsigned char) gf_exp[sumlogs];
    }
  }
}

static __global__ void premultadd_gpu(unsigned int* gf_log, unsigned int* gf_exp, int gf_size, size_t count, unsigned char* dbuf, unsigned int coeff, unsigned char* rbuf)
{
  /* TODO: read gf_log into gf_exp thread-shared memory */
  __shared__ unsigned char premult[256];

  size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < 256) {
    if (coeff != 0) {
      /* compute (v1 * v2) product as 2^( log_2(v1) + log_2(v2) ) in GF(2^bits) arithmetic */
      if (i != 0) {
        int sumlogs = gf_log[coeff] + gf_log[i];
        if (sumlogs >= gf_size - 1) {
          sumlogs -= (gf_size - 1);
        }
        premult[i] = (unsigned char) gf_exp[sumlogs];
      } else {
        premult[i] = (unsigned char) 0;
      }
    } else {
      premult[i] = (unsigned char) 0;
    }
  }
  __syncthreads();

  //size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < count) {
    int data = (int) rbuf[i];
    dbuf[i] ^= premult[data];
  }
}

static __global__ void multadd2_gpu(unsigned int* gf_log, unsigned int* gf_exp, int gf_size, size_t count, unsigned char* dbuf, unsigned int coeff, unsigned char* rbuf)
{
  /* TODO: read gf_log into gf_exp thread-shared memory */
  __shared__ unsigned char logs[256];
  __shared__ unsigned char exps[256];
  //__shared__ unsigned char exps[512];

  size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < 256) {
    logs[i] = (unsigned char) gf_log[i];
    exps[i] = (unsigned char) gf_exp[i];
  }
  //else if (i < 512) {
  //  exps[i] = (unsigned char) gf_exp[i - 255];
  //}
  __syncthreads();

  //size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  //if (i < count && coeff != 0) {
  if (i < count) {
    /* 0 times anything is 0, we treat this as a special case since
     * there is no entry for 0 in the log table below, since there
     * is no value of x such that 2^x = 0 */
    int data = rbuf[i];
    if (data != 0) {
      /* compute (v1 * v2) product as 2^( log_2(v1) + log_2(v2) ) in GF(2^bits) arithmetic */
      int sumlogs = logs[coeff] + logs[data];
      if (sumlogs >= gf_size - 1) {
        sumlogs -= (gf_size - 1);
      }
      dbuf[i] ^= (unsigned char) exps[sumlogs];
    }
  }
}

static __global__ void scale_gpu(unsigned int* gf_log, unsigned int* gf_exp, int gf_size, size_t count, unsigned char* dbuf, unsigned int coeff)
{
  /* TODO: read gf_log into gf_exp thread-shared memory */

  size_t i = blockDim.x * blockIdx.x + threadIdx.x;
  if (i < count && coeff != 0) {
    /* 0 times anything is 0, we treat this as a special case since
     * there is no entry for 0 in the log table below, since there
     * is no value of x such that 2^x = 0 */
    int data = dbuf[i];
    if (data != 0) {
      /* compute (v1 * v2) product as 2^( log_2(v1) + log_2(v2) ) in GF(2^bits) arithmetic */
      int sumlogs = gf_log[coeff] + gf_log[data];
      if (sumlogs >= gf_size - 1) {
        sumlogs -= (gf_size - 1);
      }
      dbuf[i] = (unsigned char) gf_exp[sumlogs];
    }
  }
}

/* apply ReedSolomon redundancy scheme to dataset files */
int redset_reedsolomon_encode_gpu(
  const redset_base* d,
  redset_lofi rsf,
  const char* chunk_file,
  int fd_chunk,
  size_t chunk_size)
{
  int i;

  int rc = REDSET_SUCCESS;

  /* get pointer to RS state structure */
  redset_reedsolomon* state = (redset_reedsolomon*) d->state;

  /* get offset into file immediately following the header */
  off_t header_size = lseek(fd_chunk, 0, SEEK_CUR);

  /* allocate buffer to read a piece of my file */
  unsigned char** host_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* host_buf = host_bufs[0];

  /* copy GF log and exp tables to GPU */
  unsigned int* gf_log;
  unsigned int* gf_exp;
  size_t table_size = state->gf_size * sizeof(unsigned int);
  cudaMalloc(&gf_log, table_size);
  cudaMalloc(&gf_exp, table_size);
  cudaMemcpy(gf_log, state->gf_log, table_size, cudaMemcpyHostToDevice);
  cudaMemcpy(gf_exp, state->gf_exp, table_size, cudaMemcpyHostToDevice);

  /* allocate send and receive buffers, and data buffer to accumulate result */
  unsigned char* data_bufs;
  unsigned char* recv_bufs;
  unsigned char* send_bufs;
  cudaMalloc((void**)&data_bufs, redset_mpi_buf_size * state->encoding);
  cudaMalloc((void**)&recv_bufs, redset_mpi_buf_size * state->encoding);
  cudaMalloc((void**)&send_bufs, redset_mpi_buf_size * 1);
  unsigned char* sbuf = send_bufs;

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
    cudaMemset(data_bufs, 0, redset_mpi_buf_size * state->encoding);

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
      if (redset_lofi_pread(&rsf, host_buf, count, offset) != REDSET_SUCCESS)
      {
        /* read failed, make sure we fail this rebuild */
        rc = REDSET_FAILURE;
      }

      /* TODO: send straight from host buffer to avoid memcpy */
      /* copy file data from host to device */
      cudaMemcpy(sbuf, host_buf, count, cudaMemcpyHostToDevice);

      /* send data from our file to k ranks, and receive
       * incoming data from k ranks */
      int k = 0;
      for (i = 0; i < state->encoding; i++) {
        /* distance we're sending or receiving in this round */
        int dist = d->ranks - chunk_step + i;

        /* receive data from the right */
        int rhs_rank = (d->rank + dist + d->ranks) % d->ranks;
        unsigned char* rbuf = recv_bufs + i * redset_mpi_buf_size;
        MPI_Irecv(rbuf, count, MPI_BYTE, rhs_rank, 0, d->comm, &request[k]);
        k++;

        /* send our data to the left */
        int lhs_rank = (d->rank - dist + d->ranks) % d->ranks;
        MPI_Isend(sbuf, count, MPI_BYTE, lhs_rank, 0, d->comm, &request[k]);
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
        unsigned char* dbuf = data_bufs + i * redset_mpi_buf_size;
        unsigned char* rbuf = recv_bufs + i * redset_mpi_buf_size;
        int nthreads = 1024;
        int nblocks = (count + nthreads - 1) / nthreads;
        multadd_gpu<<<nblocks, nthreads>>>(gf_log, gf_exp, state->gf_size, count, dbuf, coeff, rbuf);
        //if (coeff != 0) {
        //  multadd2_gpu<<<nblocks, nthreads>>>(gf_log, gf_exp, state->gf_size, count, dbuf, coeff, rbuf);
        //}
        //premultadd_gpu<<<nblocks, nthreads>>>(gf_log, gf_exp, state->gf_size, count, dbuf, coeff, rbuf);
      }

      cudaDeviceSynchronize();
    }

    /* write final encoded data to our chunk file */
    for (i = 0; i < state->encoding; i++) {
      unsigned char* dbuf = data_bufs + i * redset_mpi_buf_size;
      cudaMemcpy(host_buf, dbuf, count, cudaMemcpyDeviceToHost);

      off_t offset = i * chunk_size + nread + header_size;
      if (redset_lseek(chunk_file, fd_chunk, offset, SEEK_SET) != REDSET_SUCCESS) {
        rc = REDSET_FAILURE;
      }

      if (redset_write_attempt(chunk_file, fd_chunk, host_buf, count) != count) {
        rc = REDSET_FAILURE;
      }
    }

    nread += count;
  }

  redset_free(&request);
  redset_free(&status);

  cudaFree(data_bufs);
  cudaFree(recv_bufs);
  cudaFree(send_bufs);
  data_bufs = NULL;
  recv_bufs = NULL;
  send_bufs = NULL;

  cudaFree(gf_exp);
  cudaFree(gf_log);
  gf_exp = NULL;
  gf_log = NULL;

  /* free buffers */
  redset_buffers_free(1, &host_bufs);

  return rc;
}

static void redset_rs_reduce_decode_gpu(
  int ranks,
  redset_reedsolomon* state,
  unsigned int* gf_log,
  unsigned int* gf_exp,
  int chunk_id,
  int received_rank,
  int missing,
  int* rows,
  int count,
  unsigned char* recv_buf,
  unsigned char* data_bufs_dev)
{
  int i;

  /* determine encoding block this rank is responsible for in this chunk */
  int received_enc = redset_rs_get_encoding_id(ranks, state->encoding, received_rank, chunk_id);
  if (received_enc < ranks) {
    /* the data we received from this rank constitues actual data,
     * so we need to encode it by adding it to our sum */
    for (i = 0; i < missing; i++) {
      /* identify row for the data buffer in the encoding matrix,
       * then select the matrix element for the given rank,
       * finally mutiply recieved data by that coefficient and add
       * it to the data buffer */
      int row = rows[i] + ranks;
      unsigned int coeff = state->mat[row * ranks + received_rank];

      unsigned char* dbuf = data_bufs_dev + i * redset_mpi_buf_size;
      int nthreads = 1024;
      int nblocks = (count + nthreads - 1) / nthreads;
      multadd_gpu<<<nblocks, nthreads>>>(gf_log, gf_exp, state->gf_size, count, dbuf, coeff, recv_buf);
    }
  } else {
    /* in this case, the rank is responsible for holding a
     * checksum block */
    for (i = 0; i < missing; i++) {
      /* get encoding row for the current data buffer */
      int row = rows[i] + ranks;
      if (row == received_enc) {
        /* in this case, we have the checksum, just add it in */
        unsigned char* dbuf = data_bufs_dev + i * redset_mpi_buf_size;
        int nthreads = 1024;
        int nblocks = (count + nthreads - 1) / nthreads;
        add_gpu<<<nblocks, nthreads>>>(dbuf, recv_buf, count);
      } else {
        /* otherwise, this rank would have contributed
         * 0-data for this chunk and for the selected encoding row */
      }
    }
  }

  cudaDeviceSynchronize();

  return;
}

/* computed product of v1 * v2 using log and inverse log table lookups */
static unsigned int gf_mult_table_gpu(const redset_reedsolomon* state, unsigned int v1, unsigned int v2)
{
  /* 0 times anything is 0, we treat this as a special case since
   * there is no entry for 0 in the log table below, since there
   * is no value of x such that 2^x = 0 */
  if (v1 == 0 || v2 == 0) {
    return 0;
  }

  /* compute (v1 * v2) product as 2^( log_2(v1) + log_2(v2) ) in GF(2^bits) arithmetic */
  int sumlogs = state->gf_log[v1] + state->gf_log[v2];
  if (sumlogs >= state->gf_size - 1) {
    sumlogs -= (state->gf_size - 1);
  }
  int prod = state->gf_exp[sumlogs];

#if 0
  if (v1 >= state->gf_size ||
      v2 >= state->gf_size ||
      sumlogs >= state->gf_size - 1)
  {
    printf("ERRROR!!!!!\n");  fflush(stdout);
  }
#endif

  return prod;
}

/* scales a row r in a coefficient matrix in mat of size (rows x cols)
 * and an array of count values given in buf by a constant value val */
static void scale_row_gpu(
  redset_reedsolomon* state,
  unsigned int* gf_log,
  unsigned int* gf_exp,
  unsigned int* mat,  /* coefficient matrix */
  int rows,           /* number of rows in mat */
  int cols,           /* number of cols in mat */
  unsigned int val,   /* constant to multiply elements by */
  int r,              /* row within mat to be scaled by val */
  int count,          /* number of elements in buf */
  unsigned char* buf) /* list of values to be scaled by val */
{
  /* scale values across given row */
  int col;
  for (col = 0; col < cols; col++) {
    mat[r * cols + col] = gf_mult_table_gpu(state, val, mat[r * cols + col]);
  }

  /* scale all values in buffer */
  int nthreads = 1024;
  int nblocks = (count + nthreads - 1) / nthreads;
  scale_gpu<<<nblocks, nthreads>>>(gf_log, gf_exp, state->gf_size, count, buf, val);

  return;
}

/* multiply row a by the constant val, and add to row b in matrix,
 * and multiply elements in bufa and add to bufb element wise */
static void mult_add_row_gpu(
  redset_reedsolomon* state,
  unsigned int* gf_log,
  unsigned int* gf_exp,
  unsigned int* mat,
  int rows,
  int cols,
  unsigned int val,
  int a,
  int b,
  int count,
  unsigned char* bufa,
  unsigned char* bufb)
{
  /* no need to do anything if we've zero'd out the row we're adding */
  if (val == 0) {
    return;
  }

  /* multiply row a by val and add to row b */
  int col;
  for (col = 0; col < cols; col++) {
    mat[b * cols + col] ^= (unsigned char) gf_mult_table_gpu(state, val, mat[a * cols + col]);
  }

  /* multiply values in bufa by val and add to bufb */
  int nthreads = 1024;
  int nblocks = (count + nthreads - 1) / nthreads;
  multadd_gpu<<<nblocks, nthreads>>>(gf_log, gf_exp, state->gf_size, count, bufb, val, bufa);

  return;
}

/* given matrix in mat of size (rows x cols) swap columns a and b */
static void swap_columns_gpu(unsigned int* mat, int rows, int cols, int a, int b)
{
  /* nothing to do if source and destination columns are the same */
  if (a == b) {
    return;
  }

  /* otherwise march down row and swap elements between column a and b */
  int row;
  for (row = 0; row < rows; row++) {
    unsigned int val = mat[row * cols + a];
    mat[row * cols + a] = mat[row * cols + b];
    mat[row * cols + b] = val;
  }
}

/* solve for x in Ax = b, where A (given in m) is a matrix of size (missing x missing)
 * using Gaussian elimination to convert A into an identity matrix,
 * here x and b are really matrices of size [missing, count] for count number of
 * individual [missing, 1] vectors */
static void redset_rs_gaussian_solve_gpu(
  redset_reedsolomon* state,
  unsigned int* gf_log,
  unsigned int* gf_exp,
  unsigned int* m,      /* coefficient matrix to be reduced to an identity matrix */
  int missing,          /* number of rows and columns in m */
  int count,            /* length of buf arrays */
  unsigned char* bufs)  /* at list of count values for each of the missing unknowns */
{
  /* zero out lower portion of matrix */
  int row;
  for (row = 0; row < missing; row++) {
    /* search for first element in current row that is non-zero */
    int col;
    int nonzero = row;
    for (col = row; col < missing; col++) {
      unsigned int val = m[row * missing + col];
      if (val > 0) {
        nonzero = col;
        break;
      }
    }

    /* swap columns to ensure we have a nonzero in current starting position */
    swap_columns_gpu(m, missing, missing, row, nonzero);

    /* scale current row to start with a 1 */
    unsigned int val = m[row * missing + row];
    if (val != 0) {
      unsigned int imult = state->gf_imult[val];
      unsigned char* dbuf = bufs + row * redset_mpi_buf_size;
      scale_row_gpu(state, gf_log, gf_exp, m, missing, missing, imult, row, count, dbuf);
      cudaDeviceSynchronize();
    }

    /* subtract current row from each row below to zero out any leading 1 */
    int r;
    for (r = row + 1; r < missing; r++) {
      /* multiply the target row by the leading term and subtract from the current row */
      unsigned int val = m[r * missing + row];
      unsigned char* abuf = bufs + row * redset_mpi_buf_size;
      unsigned char* bbuf = bufs + r   * redset_mpi_buf_size;
      mult_add_row_gpu(state, gf_log, gf_exp, m, missing, missing, val, row, r, count, abuf, bbuf);
    }
    cudaDeviceSynchronize();
  }

  /* zero out upper portion of matrix */
  for (row = missing - 1; row > 0; row--) {
    /* for each row, compute factor needed to cancel out entry in current column
     * multiply target row and subtract from current row */
    int r;
    for (r = row - 1; r >= 0; r--) {
      /* multiply the target row by the leading term and subtract from the current row */
      unsigned int val = m[r * missing + row];
      unsigned char* abuf = bufs + row * redset_mpi_buf_size;
      unsigned char* bbuf = bufs + r   * redset_mpi_buf_size;
      mult_add_row_gpu(state, gf_log, gf_exp, m, missing, missing, val, row, r, count, abuf, bbuf);
    }
    cudaDeviceSynchronize();
  }

  return;
}

/* given a filemap, a redundancy descriptor, a dataset id, and a failed rank in my xor set,
 * rebuild files and add them to the filemap */
int redset_reedsolomon_decode_gpu(
  const redset_base* d,
  int missing,
  int* rebuild_ranks,
  int need_rebuild,
  redset_lofi rsf,
  const char* chunk_file,
  int fd_chunk,
  size_t chunk_size)
{
  int i;

  int rc = REDSET_SUCCESS;

  /* get pointer to RS state structure */
  redset_reedsolomon* state = (redset_reedsolomon*) d->state;

  /* get offset into file immediately following the header */
  off_t header_size = lseek(fd_chunk, 0, SEEK_CUR);

  /* allocate buffer to compute result of encoding,
   * we need one for each missing rank */
  unsigned char** data_bufs = (unsigned char**) redset_buffers_alloc(missing, redset_mpi_buf_size);

  /* allocate buffer to read a piece of my file */
  unsigned char** host_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* host_buf = host_bufs[0];

  /* allocate buffer to read a piece of the recevied chunk file,
   * we might get a message from each rank */
  unsigned char** recv_bufs = (unsigned char**) redset_buffers_alloc(d->ranks, redset_mpi_buf_size);
  unsigned char* rbuf = recv_bufs[0];

  unsigned int* gf_log;
  unsigned int* gf_exp;
  size_t table_size = state->gf_size * sizeof(unsigned int);
  cudaMalloc(&gf_log, table_size);
  cudaMalloc(&gf_exp, table_size);
  cudaMemcpy(gf_log, state->gf_log, table_size, cudaMemcpyHostToDevice);
  cudaMemcpy(gf_exp, state->gf_exp, table_size, cudaMemcpyHostToDevice);

  unsigned char* data_bufs_dev;
  unsigned char* recv_bufs_dev;
  cudaMalloc((void**)&data_bufs_dev, redset_mpi_buf_size * missing);
  cudaMalloc((void**)&recv_bufs_dev, redset_mpi_buf_size * d->ranks);

  unsigned char* send_buf_dev;
  cudaMalloc(&send_buf_dev, redset_mpi_buf_size);

  /* switch send/recv to use device buffers */
  rbuf = recv_bufs_dev;
  unsigned char* sbuf = send_buf_dev;

  /* this array will map from missing rank number to missing data segment id,
   * which falls in the range [0, d->ranks + state->encoding),
   * we'll have one value for each missing rank */
  int* unknowns = (int*) REDSET_MALLOC(missing * sizeof(int));

  /* we'll have each process solve for the chunk matching its rank number */
  int decode_chunk_id = d->rank;
  for (i = 0; i < missing; i++) {
    int missing_rank = rebuild_ranks[i];
    unknowns[i] = redset_rs_get_encoding_id(d->ranks, state->encoding, missing_rank, decode_chunk_id);
  }

  /* given the ids of the unknown values,
   * pick among the available encoding rows for the quickest solve */
  unsigned int* m = NULL;
  int* rows = NULL;
  redset_rs_gaussian_solve_identify_rows(state, state->mat, d->ranks, state->encoding,
    missing, unknowns, &m, &rows
  );

  /* make a copy of the matrix coeficients */
  unsigned int* mcopy = (unsigned int*) REDSET_MALLOC(missing * missing * sizeof(unsigned int));
  
  /* during the reduce-scatter phase, each process has 1 outstanding send/recv at a time,
   * at the end, each process sends data to each failed rank and failed ranks receive a
   * message from all ranks, this allocation is more than needed */
  int max_outstanding = (d->ranks + state->encoding) * 2;
  MPI_Request* request = (MPI_Request*) REDSET_MALLOC(max_outstanding * sizeof(MPI_Request));
  MPI_Status*  status  = (MPI_Status*)  REDSET_MALLOC(max_outstanding * sizeof(MPI_Status));

  /* process all data for this chunk */
  size_t nread = 0;
  while (nread < chunk_size) {
    /* limit the amount of data we read from the file at a time */
    size_t count = chunk_size - nread;
    if (count > redset_mpi_buf_size) {
      count = redset_mpi_buf_size;
    }

    /* initialize buffers to accumulate reduction results */
    cudaMemset(data_bufs_dev, 0, redset_mpi_buf_size * missing);

    int step_id;
    for (step_id = 0; step_id < d->ranks; step_id++) {
      int lhs_rank = (d->rank - step_id + d->ranks) % d->ranks;
      int rhs_rank = (d->rank + step_id + d->ranks) % d->ranks;

      /* get id of chunk we'll be sending in this step */
      int chunk_id = (d->rank + step_id) % d->ranks;

      /* get row number of encoding matrix we used for this chunk */
      int enc_id = redset_rs_get_encoding_id(d->ranks, state->encoding, d->rank, chunk_id);

      /* prepare our input buffers for the reduction */
      if (! need_rebuild) {
        /* we did not fail, so we can read data from our files,
         * determine whether we read from data files or chunk file */
        if (enc_id < d->ranks) {
          /* compute offset to read from within our file */
          int chunk_id_rel = redset_rs_get_data_id(d->ranks, state->encoding, d->rank, chunk_id);
          unsigned long offset = chunk_size * (unsigned long) chunk_id_rel + nread;

          /* read data from our file */
          if (redset_lofi_pread(&rsf, host_buf, count, offset) != REDSET_SUCCESS)
          {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        } else {
          /* for this chunk, read data from the chunk file */
          off_t offset = (enc_id - d->ranks) * chunk_size + nread + header_size;
          if (redset_lseek(chunk_file, fd_chunk, offset, SEEK_SET) != REDSET_SUCCESS) {
            /* seek failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
          if (redset_read_attempt(chunk_file, fd_chunk, host_buf, count) != count) {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        }
      } else {
        /* if we're rebuilding, initialize our send buffer with 0,
         * so that our input does not contribute to the result */
        memset(host_buf, 0, count);
      }

      /* pipelined reduce-scatter across ranks */
      if (step_id > 0) {
        /* TODO: send straight from host buffer to avoid memcpy */
        /* copy file data from host to device */
        cudaMemcpy(sbuf, host_buf, count, cudaMemcpyHostToDevice);

        /* exchange data with neighboring ranks */
        MPI_Irecv(rbuf, count, MPI_BYTE, lhs_rank, 0, d->comm, &request[0]);
        MPI_Isend(sbuf, count, MPI_BYTE, rhs_rank, 0, d->comm, &request[1]);
        MPI_Waitall(2, request, status);
      } else {
        /* if we're rebuilding, initialize our send buffer with 0,
         * so that our input does not contribute to the result */
        /* copy file data from host to device */
        cudaMemcpy(rbuf, host_buf, count, cudaMemcpyHostToDevice);
      }

      /* merge received blocks via xor operation */
      redset_rs_reduce_decode_gpu(d->ranks, state, gf_log, gf_exp, decode_chunk_id, lhs_rank, missing, rows, count, rbuf, data_bufs_dev);
    }

    /* at this point, we need to invert our m matrix to solve for unknown values,
     * we invert a copy because we need to do this operation multiple times */
    memcpy(mcopy, m, missing * missing * sizeof(unsigned int));
    redset_rs_gaussian_solve_gpu(state, gf_log, gf_exp, mcopy, missing, count, data_bufs_dev);
    for (i = 0; i < missing; i++) {
      unsigned char* dbuf = data_bufs_dev + i * redset_mpi_buf_size;
      cudaMemcpy(data_bufs[i], dbuf, redset_mpi_buf_size, cudaMemcpyDeviceToHost);
    }

    /* TODO: for large groups, we may want to add some flow control here */

    /* send back our results to the failed ranks, just let it all fly */
    int k = 0;

    /* if we need to rebuild, post a receive from every other rank,
     * we stagger them based on our rank to support a natural ring */
    if (need_rebuild) {
      for (step_id = 0; step_id < d->ranks; step_id++) {
        int lhs_rank = (d->rank - step_id + d->ranks) % d->ranks;
        MPI_Irecv(recv_bufs[lhs_rank], count, MPI_BYTE, lhs_rank, 0, d->comm, &request[k]);
        k++;
      }
    }

    /* send the segments we rebuilt to each failed rank */
    for (i = 0; i < missing; i++) {
      int missing_rank = rebuild_ranks[i];
      MPI_Isend(data_bufs[i], count, MPI_BYTE, missing_rank, 0, d->comm, &request[k]);
      k++;
    }

    /* wait for all comms to finish */
    MPI_Waitall(k, request, status);

    /* if we need to rebuild, we now have data we can write to our files */
    if (need_rebuild) {
      for (step_id = 0; step_id < d->ranks; step_id++) {
        /* pick a rank to walk through our file */
        int lhs_rank = (d->rank - step_id + d->ranks) % d->ranks;

        /* at this point, we have the final result in our data buffers,
         * so we can write it out to the files */
        int received_chunk_id = lhs_rank;
        int enc_id = redset_rs_get_encoding_id(d->ranks, state->encoding, d->rank, received_chunk_id);
        if (enc_id < d->ranks) {
          /* for this chunk, write data to the logical file */
          int chunk_id_rel = redset_rs_get_data_id(d->ranks, state->encoding, d->rank, received_chunk_id);
          unsigned long offset = chunk_size * (unsigned long) chunk_id_rel + nread;
          if (redset_lofi_pwrite(&rsf, recv_bufs[lhs_rank], count, offset) != REDSET_SUCCESS)
          {
            /* write failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        } else {
          /* write send block to chunk file */
          off_t offset = (enc_id - d->ranks) * chunk_size + nread + header_size;
          if (redset_lseek(chunk_file, fd_chunk, offset, SEEK_SET) != REDSET_SUCCESS) {
            rc = REDSET_FAILURE;
          }
          if (redset_write_attempt(chunk_file, fd_chunk, recv_bufs[lhs_rank], count) != count) {
            rc = REDSET_FAILURE;
          }
        }
      }
    }

    nread += count;
  }

  /* free off MPI requests */
  redset_free(&request);
  redset_free(&status);

  /* free matrix coefficients and selected rows needed to decode */
  redset_free(&mcopy);
  redset_free(&m);
  redset_free(&rows);

  cudaFree(data_bufs_dev);
  cudaFree(recv_bufs_dev);
  cudaFree(send_buf_dev);
  cudaFree(gf_exp);
  cudaFree(gf_log);
  data_bufs_dev = NULL;
  recv_bufs_dev = NULL;
  send_buf_dev = NULL;
  gf_exp = NULL;
  gf_log = NULL;

  /* free buffers */
  redset_buffers_free(missing,  &data_bufs);
  redset_buffers_free(1,        &host_bufs);
  redset_buffers_free(d->ranks, &recv_bufs);

  return rc;
}

#ifdef __cplusplus
} /* extern C */
#endif
