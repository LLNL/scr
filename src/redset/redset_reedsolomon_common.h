#ifndef REDSET_REEDSOLOMON_COMMON_H
#define REDSET_REEDSOLOMON_COMMON_H

#ifdef __cplusplus
extern "C" {
#endif

/* given a pointer to a reed-solomon state structure,
 * number of ranks, requested encoding checksums, and number of bits
 * to determine the number of Galois Field elements, allocate structures
 * that define the Galois Field */
int redset_rs_gf_alloc(
  redset_reedsolomon* state,
  int ranks,    /* number of ranks (data items) */
  int encoding, /* number of checksum encodings */
  int bits      /* define bits for Galois Field GF(2^bits) */
);

/* free memory allocated and attached to state
 * from an earlier call to redset_rs_gf_alloc */
void redset_rs_gf_delete(
  redset_reedsolomon* state
);

void gf_premult_table(
  const redset_reedsolomon* state,
  unsigned int v1,
  unsigned char* prods
);

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
  int** selected         /* row index selected from encoding matrix for each unknown */
);

/* solve for x in Ax = b, where A (given in m) is a matrix of size (missing x missing)
 * using Gaussian elimination to convert A into an identity matrix,
 * here x and b are really matrices of size [missing, count] for count number of
 * individual [missing, 1] vectors */
void redset_rs_gaussian_solve(
  redset_reedsolomon* state,
  unsigned int* m,     /* coefficient matrix to be reduced to an identity matrix */
  int missing,         /* number of rows and columns in m */
  int count,           /* length of buf arrays */
  unsigned char** bufs /* at list of count values for each of the missing unknowns */
);

/* return encoding index [0, ranks+encoding) for given rank and chunk_id */
int redset_rs_get_encoding_id(
  int ranks,
  int encoding,
  int rank,
  int chunk_id
);

/* return chunk id into logical file for given rank and chunk_id */
int redset_rs_get_data_id(
  int ranks,
  int encoding,
  int rank,
  int chunk_id
);

/* multiply elements in data by coeff and add to buf */
void redset_rs_reduce_buffer_multadd(
  redset_reedsolomon* state,
  int count,          /* length of buffer and data arrays */
  unsigned char* buf, /* accumulation buffer */
  unsigned int coeff, /* constant to scale elements in data by */
  unsigned char* data /* items to be multiplied and added to buf */
);

void redset_rs_reduce_decode(
  int ranks,
  redset_reedsolomon* state,
  int chunk_id,
  int received_rank,
  int missing,
  int* rows,
  int count,
  unsigned char* recv_buf,
  unsigned char** data_bufs
);

#ifdef __cplusplus
} /* extern C */
#endif

#endif
