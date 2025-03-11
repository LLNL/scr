#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"

#ifdef HAVE_OPENMP
#include <omp.h>
#endif /* HAVE_OPENMP */

#ifdef REDSET_ENABLE_MPI
#include "mpi.h"
#endif

#include "kvtree.h"
#include "kvtree_util.h"
#ifdef REDSET_ENABLE_MPI
#include "kvtree_mpi.h"
#endif

#include "redset_io.h"
#include "redset_util.h"
#include "redset.h"
#include "redset_internal.h"

#define REDSET_KEY_COPY_RS_DESC  "DESC"
#define REDSET_KEY_COPY_RS_CHUNK "CHUNK"
#define REDSET_KEY_COPY_RS_CKSUM "CKSUM"

/*
=========================================
Distribute and file rebuild functions
=========================================
*/

/* multiply v1 * v2 in GF(2^m) arithmetic given the generating polynomial in poly */
static int gf_mult(
  int m,    // GF(2 ^ M)
  int poly, // low order terms of g(x)
  int v1,
  int v2)
{
  int prod = 0;

  /* Multiply phase */
  int k;
  for (k = 0; k < m; k++) {
    if (v1 & 1) {
      prod ^= (v2 << k);
    }

    v1 >>= 1;

    if (v1 == 0) {
      break;
    }
  }

  /* Reduce phase */
  int mask = 1 << m;
  mask <<= m - 2;
  for (k = m - 2; k >= 0; k--) {
    if (prod & mask) {
      prod &= ~mask;
      prod ^= (poly << k);
    }
    mask >>= 1;
  }

  return prod;
}

/* construct tables to compute log_2(x) and 2^x for each
 * value of x in the range [0,2^(bits)) that makes up a GF(2^bits) */
static void gf_build_tables(redset_reedsolomon* state, int bits)
{
  /* record number of bits we're using and number of elements 2^bits */
  state->gf_bits = bits;
  state->gf_size = 1 << bits;

  /* number of elements in our Galois Field */
  int size = state->gf_size;

  /* define remainder term of our irreducible polynomial */
  int poly = 0;
  if (bits == 3) {
    poly = 0x3; /* x^3 + ((x + 1)) */
  } else if (bits == 4) {
    poly = 0x3; /* x^4 + ((x + 1)) */
  } else if (bits == 8) {
    poly = 0x1D; /* x^8 + ((x^4 + x^3 + x^2 + 1)) */
  }

  /* allocate space to hold log(i) and exp(i) for i in [0,size) */
  state->gf_log   = (unsigned int*) REDSET_MALLOC(size * sizeof(unsigned int));
  state->gf_exp   = (unsigned int*) REDSET_MALLOC(size * sizeof(unsigned int));
  state->gf_imult = (unsigned int*) REDSET_MALLOC(size * sizeof(unsigned int));

  /* allocate space to pre-compute the producet of every element in our field
   * with a given constant, this will be used to speed up the process of
   * multiplying a long string of numbers by a constant */
  if (state->gf_size <= 256) {
    state->gf_premult = (unsigned char*) REDSET_MALLOC(state->gf_size * sizeof(unsigned char));
  }

  /* log(0) and exp(size-1) are undefined,
   * but init them to 0 just to have something */
  state->gf_log[0]        = 0;
  state->gf_exp[size - 1] = 0;

  /* define log(1)=0 and exp(0)=1 */
  state->gf_log[1] = 0;
  state->gf_exp[0] = 1;

  /* first element in inverse log table
   * is exp(1) with 2 as base, and 2^1 = 2 */
  int prod = 2;

  /* multiply by powers of 2, using GF(2^bits) arithmetic */
  int i;
  for (i = 1; i < size - 1; i++) {
    state->gf_exp[i] = prod;

    /* define log(prod)=i, to be inverse of above */
    state->gf_log[prod] = i;

    prod = gf_mult(bits, poly, prod, 2);
  }

  /* zero has no multiplicative inverse, but initialize this value to 0 anyway */
  state->gf_imult[0] = 0;

  /* for each value, find its multiplicative inverse */
  for (i = 1; i < size; i++) {
    int j;
    for (j = 1; j < size; j++) {
      prod = gf_mult(bits, poly, i, j);
      if (prod == 1) {
        state->gf_imult[i] = j;
        break;
      }
    }
  }

  return;
}

/* computed product of v1 * v2 using log and inverse log table lookups */
static unsigned int gf_mult_table(const redset_reedsolomon* state, unsigned int v1, unsigned int v2)
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

/* precompute the product of a constant v1 with every value v2 in our Galois Field,
 * and store the result in a lookup table at location v2,
 * given a value v2, one can then later compute v1*v2 with a lookup using v2 as the index */
void gf_premult_table(
  const redset_reedsolomon* state,
  unsigned int v1,      /* constant value to multiply all elements by */
  unsigned char* prods) /* array of length state->gf_size to hold product of v1 with each element */
{
  int i;

  /* This optimization only works for GF(2^8) or smaller fields
   *  since our prods array is defined as a char datatype */

  /* get size of the field */
  int size = state->gf_size;

  /* if v1 is 0, v1 times anything is 0 */
  if (v1 == 0) {
    for (i = 0; i < size; i++) {
      memset(prods, 0, size);
    }
    return;
  }

  /* if v2 is 1, v1 times any value v2 is the same value */
  if (v1 == 1) {
    for (i = 0; i < size; i++) {
      prods[i] = (unsigned char) i;
    }
    return;
  }

  /* otherwise we have some work to do,
   * first lookup the log of the value v1 */
  int log_v1 = state->gf_log[v1];

  /* still, 0 times anything is 0
   * and 1 times anything is itself */
  prods[0] = 0;
  prods[1] = (unsigned char) v1;

  /* for all elements for 2 and greater,
   * we compute the product v1*v2 as 2^( log_2(v1) + log_2(v2) ) in GF(2^bits) arithmetic */
  for (i = 2; i < size; i++) {
    int sumlogs = log_v1 + state->gf_log[i];
    if (sumlogs >= state->gf_size - 1) {
      sumlogs -= (state->gf_size - 1);
    }
    prods[i] = (unsigned char) state->gf_exp[sumlogs];
  }

  return;
}

#if 0
static void print_matrix(int* mat, int rows, int cols)
{
  int row, col;
  for (row = 0; row < rows; row++) {
    for (col = 0; col < cols; col++) {
      printf("%d ", mat[row * cols + col]);
    }
    printf("\n");
  }
  printf("\n"); fflush(stdout);
}
#endif

/* given matrix in mat of size (rows x cols) swap columns a and b */
static void swap_columns(unsigned int* mat, int rows, int cols, int a, int b)
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

/* scales a row r in a coefficient matrix in mat of size (rows x cols)
 * and an array of count values given in buf by a constant value val */
static void scale_row(
  redset_reedsolomon* state,
  unsigned int* mat,  /* coefficient matrix */
  int rows,           /* number of rows in mat */
  int cols,           /* number of cols in mat */
  unsigned int val,   /* constant to multiply elements by */
  int r,              /* row within mat to be scaled by val */
  int count,          /* number of elements in buf */
  unsigned char* buf) /* list of values to be scaled by val */
{
  /* use our premultiplication table if it is defined and if the
   * number of elements in our Galois Field is smaller than the input arrays */
  if (state->gf_premult != NULL && state->gf_size < count) {
    /* precompute product of every value with given constant val */
    unsigned char* premult = state->gf_premult;
    gf_premult_table(state, val, premult);

    /* scale values across given row */
    int col;
    for (col = 0; col < cols; col++) {
      mat[r * cols + col] = premult[mat[r * cols + col]];
    }

    /* scale all values in buffer */
    int i;
    #pragma omp parallel for
    for (i = 0; i < count; i++) {
      buf[i] = premult[buf[i]];
    }

    return;
  }

  /* if we can't use the premult table to look up products,
   * we do it the hard way for each item */

  /* scale values across given row */
  int col;
  for (col = 0; col < cols; col++) {
    mat[r * cols + col] = gf_mult_table(state, val, mat[r * cols + col]);
  }

  /* scale all values in buffer */
  int i;
  #pragma omp parallel for
  for (i = 0; i < count; i++) {
    unsigned int val2 = (unsigned int) buf[i];
    buf[i] = (unsigned char) gf_mult_table(state, val, val2);
  }

  return;
}

/* adds row a to row b in given matrix,
 * and adds items in bufa to bufb element wise */
static void add_row(
  redset_reedsolomon* state,
  unsigned int* mat,
  int rows,
  int cols,
  int a,
  int b,
  int count,
  unsigned char* bufa,
  unsigned char* bufb)
{
  /* add elements in row a to row b */
  int col;
  for (col = 0; col < cols; col++) {
    mat[b * cols + col] ^= mat[a * cols + col];
  }

  /* add values in bufa to bufb */
  int i;
  #pragma omp parallel for
  for (i = 0; i < count; i++) {
    bufb[i] ^= bufa[i];
  }

  return;
}

/* multiply row a by the constant val, and add to row b in matrix,
 * and multiply elements in bufa and add to bufb element wise */
static void mult_add_row(
  redset_reedsolomon* state,
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

  /* no need to scale the row before adding */
  if (val == 1) {
    add_row(state, mat, rows, cols, a, b, count, bufa, bufb);
    return;
  }

  /* use our premultiplication table if it is defined and if the
   * number of elements in our Galois Field is smaller than the input arrays */
  if (state->gf_premult != NULL && state->gf_size < count) {
    /* precompute product of every value with given constant val */
    unsigned char* premult = state->gf_premult;
    gf_premult_table(state, val, premult);

    /* multiply row a by val and add to row b */
    int col;
    for (col = 0; col < cols; col++) {
      mat[b * cols + col] ^= premult[mat[a * cols + col]];
    }

    /* multiply values in bufa by val and add to bufb */
    int i;
    #pragma omp parallel for
    for (i = 0; i < count; i++) {
      bufb[i] ^= premult[bufa[i]];
    }

    return;
  }

  /* if we can't use the premult table to look up products,
   * we do it the hard way for each item */

  /* multiply row a by val and add to row b */
  int col;
  for (col = 0; col < cols; col++) {
    mat[b * cols + col] ^= (unsigned char) gf_mult_table(state, val, mat[a * cols + col]);
  }

  /* multiply values in bufa by val and add to bufb */
  int i;
  #pragma omp parallel for
  for (i = 0; i < count; i++) {
    bufb[i] ^= (unsigned char) gf_mult_table(state, val, bufa[i]);
  }

  return;
}

/* Given our (n+k) x n encoding matrix in mat, pick M different rows
 * with M=missing from the last k checksum rows that we can use to
 * solve for the given unknown index values which will be in the range [0,n+k).
 * Any M distinct rows suffice, so we try pick rows having a minimum
 * number of unknowns to make for a faster solve later.
 * We extract an (MxM) matrix holding the subset of coefficients from the
 * encoding matrix for the selected rows.  Inverting the MxM matrix
 * specifies the operations needed to compute the given unknowns. */
void redset_rs_gaussian_solve_identify_rows(
  redset_reedsolomon* state,
  unsigned int* mat,     /* encoding matrix */
  int n,                 /* number of ranks */
  int k,                 /* number of checksum values */
  int missing,           /* number of missing values */
  int* unknowns,         /* index of each missing value [0,n+k) */
  unsigned int** coeffs, /* output matrix of size (missing x missing) */
  int** selected)        /* row index selected from encoding matrix for each unknown */
{
  int i;
  int row;

  /* allocate a (missing x missing) matrix to hold coefficients for unknowns,
   * based on the rows we'll select */
  unsigned int* m = (unsigned int*) REDSET_MALLOC(missing * missing * sizeof(unsigned int));

  /* tracks row selected for each unknown */
  int* selected_row  = (int*) REDSET_MALLOC(missing * sizeof(int));

  /* we use the checksum portion of the matrix to pick
   * our equations to solve for the unknowns,
   * for each unknown choose a row that includes the
   * unknown having the fewest other unknowns */

  /* record number of unknowns in each checksum row */
  int* numk = (int*) REDSET_MALLOC(k * sizeof(int));

  for (row = 0; row < k; row++) {
    /* intialize count of unknowns for this row */
    numk[row] = 0;

    /* count up number of unknowns in this row */
    for (i = 0; i < missing; i++) {
      /* get index of unknown value */
      int u = unknowns[i];
      if (u < n) {
        /* unknown is a data value */
        unsigned int coeff = mat[(row + n) * n + u];
        if (coeff != 0) {
          /* coefficient for unknown in this row is not 0 */
          numk[row]++;
        }
      } else {
        /* the unknown is a checksum value */
        if (u == row + n) {
          /* for checksums, only one row has the value defined */
          numk[row]++;
        }
      }
    }
  }

  /* flags to indicate whether row has been selected */
  int* assigned = (int*) REDSET_MALLOC(k * sizeof(int));

  /* initialize all rows as available */
  for (row = 0; row < k; row++) {
    assigned[row] = 0;
  }

  /* for each missing value, pick a row where:
   *   a) it is defined
   *   b) has the least number of unknowns */
  for (i = 0; i < missing; i++) {
    /* initialize min to a value higher than any miniumum */
    int min = missing + 1;

    /* this will track the row with the minimum number
     * of unknowns */
    int best_row = -1;

    /* get index of current unknown value */
    int u = unknowns[i];

    /* check each checksum row,
     * if this unknown is defined in this row,
     * and if this row has fewer unknowns, select it */
    for (row = 0; row < k; row++) {
      /* skip this row if it's already used */
      if (assigned[row]) {
        continue;
      }

      if (u < n) {
        /* unknown is a data value */
        unsigned int coeff = mat[(row + n) * n + u];
        if (coeff != 0) {
          /* coefficient for unknown in this row is not 0,
           * so it's a valid choice to the given unknown */
          if (numk[row] < min) {
            min = numk[row];
            best_row = row;
          }
        }
      } else {
        /* the unknown is a checksum value */
        if (u == row + n) {
          /* for checksums, only one row has the value defined */
          if (numk[row] < min) {
            min = numk[row];
            best_row = row;
          }
        }
      }
    }

    /* record the row we choose for each unknown */
    selected_row[i] = best_row;
    assigned[best_row] = 1;

    /* copy coefficients for best row for this unknown into
     * our output matrix m */
    int j;
    for (j = 0; j < missing; j++) {
      int uj = unknowns[j];
      if (uj < n) {
        /* unknown is a data value, take coefficient from matrix */
        m[i * missing + j] = mat[(best_row + n) * n + uj];
      } else if (uj == best_row + n) {
        /* unknown is a checksum value, use 1 if we're in the correct row */
        m[i * missing + j] = 1;
      } else {
        /* unknown is a checksum value */
        m[i * missing + j] = 0;
      }
    }
  }

  redset_free(&assigned);
  redset_free(&numk);

//  print_matrix(mat, n+k, n);
//  print_matrix(m, missing, missing);

  *coeffs = m;
  *selected = selected_row;

  return;
}

/* solve for x in Ax = b, where A (given in m) is a matrix of size (missing x missing)
 * using Gaussian elimination to convert A into an identity matrix,
 * here x and b are really matrices of size [missing, count] for count number of
 * individual [missing, 1] vectors */
void redset_rs_gaussian_solve(
  redset_reedsolomon* state,
  unsigned int* m,      /* coefficient matrix to be reduced to an identity matrix */
  int missing,          /* number of rows and columns in m */
  int count,            /* length of buf arrays */
  unsigned char** bufs) /* at list of count values for each of the missing unknowns */
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
    swap_columns(m, missing, missing, row, nonzero);

    /* scale current row to start with a 1 */
    unsigned int val = m[row * missing + row];
    if (val != 0) {
      unsigned int imult = state->gf_imult[val];
      scale_row(state, m, missing, missing, imult, row, count, bufs[row]);
    }

    /* subtract current row from each row below to zero out any leading 1 */
    int r;
    for (r = row + 1; r < missing; r++) {
      /* multiply the target row by the leading term and subtract from the current row */
      unsigned int val = m[r * missing + row];
      mult_add_row(state, m, missing, missing, val, row, r, count, bufs[row], bufs[r]);
    }

//    print_matrix(m, missing, missing);
  }

//  print_matrix(m, missing, missing);

  /* zero out upper portion of matrix */
  for (row = missing - 1; row > 0; row--) {
    /* for each row, compute factor needed to cancel out entry in current column
     * multiply target row and subtract from current row */
    int r;
    for (r = row - 1; r >= 0; r--) {
      /* multiply the target row by the leading term and subtract from the current row */
      unsigned int val = m[r * missing + row];
      mult_add_row(state, m, missing, missing, val, row, r, count, bufs[row], bufs[r]);
    }
//    print_matrix(m, missing, missing);
  }

//  print_matrix(m, missing, missing);

  return;
}

/* this normalizes our Vandermonde matrix using column-wise Gaussian elimination
 * to convert the top n x n portion of our (n+k) x n matrix into an identity matrix */
static void normalize_vandermonde(redset_reedsolomon* state, unsigned int* mat, int n, int k)
{
  int row, col;
  for (row = 0; row < n; row++) {
    /* find a column element in the current row that is non-zero */
    int nonzero_col = -1;
    for (col = row; col < n; col++) {
      if (mat[row * n + col] != 0) {
        nonzero_col = col;
        break;
      }
    }

    /* swap column if needed */
    swap_columns(mat, n + k, n, row, nonzero_col);

    /* compute coefficient to take element (row,row) to 1 */
    unsigned int val = mat[row * n + row];
    unsigned int imult = state->gf_imult[val];

    /* scale column by factor to take (row,row) to 1 */
    int row2;
    for (row2 = row; row2 < n + k; row2++) {
      unsigned int scaled = mat[row2 * n + row];
      mat[row2 * n + row] = gf_mult_table(state, scaled, imult);
    }

    for (col = 0; col < n; col++) {
      /* skip processing the pivot column */
      if (col == row) {
        continue;
      }

      /* get value we need to multiply column by to cancel out
       * non-zero element in the current column */
      unsigned int scaled = mat[row * n + col];
      if (scaled != 0) {
        for (row2 = row; row2 < n + k; row2++) {
          unsigned int scaled_val = gf_mult_table(state, scaled, mat[row2 * n + row]);
          mat[row2 * n + col] ^= scaled_val;
        }
      }
    }

//    print_matrix(mat, n+k, n);
  }

  return;
}

/* construct a matrix of size (n+k) x k where element in row i and column j
 * is given as i^j with i and j starting from 0, then normalize the matrix
 * so the top (n x n) portion is an identity matrix, this produces an encoding
 * matrix, for example with n=4, k=2 in GF(2^8) arithmetic:
 *
 *    1  0  0  0
 *    0  1  0  0
 *    0  0  1  0
 *    0  0  0  1
 *   27 28 18 20
 *   28 27 20 18 */
static unsigned int* build_vandermonde(redset_reedsolomon* state, int n, int k)
{
  /* allocate space to hold a matrix of size (n+k) x n */
  int size = (n + k) * n;
  unsigned int* mat = (unsigned int*) REDSET_MALLOC(size * sizeof(unsigned int));

  /* initialize elements for Vandermonde for our selected Galois Field */
  int row, col;
  for (row = 0; row < n + k; row++) {
    /* first element of each column is row^0 = 1 */
    mat[row * n + 0] = 1;

    /* other column elements are successive powers of row
     * row^col in GF(2^bits) arithmetic */
    unsigned int val = (unsigned int) row;
    for (col = 1; col < n; col++) {
      mat[row * n + col] = val;
      val = gf_mult_table(state, val, row);
    }
  }

//  print_matrix(mat, n+k, n);

  /* convert to normal form using column-wise gaussian elimination */
  normalize_vandermonde(state, mat, n, k);

//  print_matrix(mat, n+k, n);

  /* return matrix to the caller */
  return mat;
}

int redset_rs_gf_alloc(
  redset_reedsolomon* state,
  int ranks,    /* number of ranks (data items) */
  int encoding, /* number of checksum encodings */
  int bits)     /* define bits for Galois Field GF(2^bits) */
{
  /* assume we'll succeed */
  int rc = REDSET_SUCCESS;

  /* set number of encoding blocks */
  state->encoding = encoding;

  /* initialize fields that define our Galois Field */
  state->gf_bits    = 0;
  state->gf_size    = 0;
  state->gf_log     = NULL;
  state->gf_exp     = NULL;
  state->gf_imult   = NULL;
  state->gf_premult = NULL;

  /* build our log and exp tables for Galois Field GF(2^8) lookups
   * to speed up multiplication operations */
  gf_build_tables(state, bits);

  /* build Vandermonde matrix (ranks + encoding x ranks) and normalize
   * the top portion to be the identify matrix, this defines the encoding
   * matrix that we'll use */
  state->mat = build_vandermonde(state, ranks, state->encoding);

  return rc;
}

void redset_rs_gf_delete(redset_reedsolomon* state)
{
  /* free memory allocated in redset_rs_gf_alloc */
  redset_free(&state->gf_log);
  redset_free(&state->gf_exp);
  redset_free(&state->gf_imult);
  redset_free(&state->gf_premult);
  redset_free(&state->mat);

  return;
}

/* element-wise add items in data to buf */
static void reduce_buffer_add(
  redset_reedsolomon* state,
  int count,           /* length of buf and data arrays */
  unsigned char* buf,  /* accumulation buffer */
  unsigned char* data) /* items to be added to buf element wise */
{
  int j;
  #pragma omp parallel for
  for (j = 0; j < count; j++) {
    buf[j] ^= data[j];
  }
}

/* multiply elements in data by coeff and add to buf */
void redset_rs_reduce_buffer_multadd(
  redset_reedsolomon* state,
  int count,           /* length of buffer and data arrays */
  unsigned char* buf,  /* accumulation buffer */
  unsigned int coeff,  /* constant to scale elements in data by */
  unsigned char* data) /* items to be multiplied and added to buf */
{
  int j;

#ifndef HAVE_PTHREADS
  /* use our premultiplication table if it is defined and if the
   * number of elements in our Galois Field is smaller than the input arrays */
  if (state->gf_premult != NULL && state->gf_size < count) {
    /* precompute product of every value with given constant val */
    unsigned char* premult = state->gf_premult;
    gf_premult_table(state, coeff, premult);

    /* now the product of coeff * and the value at val=data[j] can be
     * looked up with premult[val] */
    #pragma omp parallel for
    for (j = 0; j < count; j++) {
      buf[j] ^= premult[data[j]];
    }

    return;
  }
#endif /* HAVE_PTHREADS */

  /* otherwise, fall back to lookup each product in our log tables */
  #pragma omp parallel for
  for (j = 0; j < count; j++) {
    buf[j] ^= (unsigned char) gf_mult_table(state, coeff, data[j]);
  }
}

/* return encoding index [0, ranks+encoding) for given rank and chunk_id */
int redset_rs_get_encoding_id(int ranks, int encoding, int rank, int chunk_id)
{
  /* index within encoding matrix for missing chunk on this rank */
  int num_segments = ranks - encoding;
  int id = (num_segments - rank + ranks + chunk_id) % ranks;
  if (id < num_segments) {
    id = rank;
  } else {
    id = ranks + (id - num_segments);
  }
  return id;
}

/* return chunk id into data files for given rank and chunk_id */
int redset_rs_get_data_id(int ranks, int encoding, int rank, int chunk_id)
{
  /* read from our data files for this chunk,
   * compute data chunk we should read for this rank on this step */
  int id = chunk_id;
  if (id > rank) {
    id -= encoding;
  }

  /* adjust for any leading encoding blocks that come before our data blocks */
  int last_enc_id = rank + encoding;
  int lead_chunks = last_enc_id - ranks;
  if (lead_chunks > 0) {
    id -= lead_chunks;
  }

  return id;
}

void redset_rs_reduce_decode(
  int ranks,
  redset_reedsolomon* state,
  int chunk_id,
  int received_rank,
  int missing,
  int* rows,
  int count,
  unsigned char* recv_buf,
  unsigned char** data_bufs)
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
      redset_rs_reduce_buffer_multadd(state, count, data_bufs[i], coeff, recv_buf);
    }
  } else {
    /* in this case, the rank is responsible for holding a
     * checksum block */
    for (i = 0; i < missing; i++) {
      /* get encoding row for the current data buffer */
      int row = rows[i] + ranks;
      if (row == received_enc) {
        /* in this case, we have the checksum, just add it in */
        reduce_buffer_add(state, count, data_bufs[i], recv_buf);
      } else {
        /* otherwise, this rank would have contributed
         * 0-data for this chunk and for the selected encoding row */
      }
    }
  }

  return;
}
