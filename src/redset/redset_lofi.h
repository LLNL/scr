#ifndef REDSET_LOFI_H
#define REDSET_LOFI_H

#ifdef __cplusplus
extern "C" {
#endif

/* structure to track an ordered set of files and operate on
 * them as one logical, continuous file */
typedef struct {
  int numfiles;             /* number of files in list */
  off_t bytes;              /* number of bytes summed across all files */
  int* fds;                 /* file descriptor for each file */
  const char** filenames;   /* name of each file */
  unsigned long* filesizes; /* size of each file */
} redset_lofi;

/* encode file info into kvtree */
int redset_lofi_encode_kvtree(kvtree* hash, int num, const char** files);

/* encode a map of source file to destination file,
 * can be passed to open to look at destination rather than source for each data file */
int redset_lofi_encode_map(kvtree* hash, int num, const char** src_files, const char** dst_files);

/* check whether files in kvtree exist and match expected properties */
int redset_lofi_check_mapped(const kvtree* hash, const kvtree* map);

/* check whether files in kvtree exist and match expected properties */
int redset_lofi_check(const kvtree* hash);

/* given a hash that defines a set of files, open our logical file for reading */
int redset_lofi_open_mapped(const kvtree* hash, const kvtree* map, int flags, mode_t mode, redset_lofi* rsf);

/* given a hash that defines a set of files, open our logical file for reading */
int redset_lofi_open(const kvtree* hash, int flags, mode_t mode, redset_lofi* rsf);

/* return file size of our logical file */
unsigned long redset_lofi_bytes(redset_lofi* rsf);

/* read from logical file */
int redset_lofi_pread(redset_lofi* rsf, void* buf, size_t count, off_t offset);

/* write to logical file */
int redset_lofi_pwrite(redset_lofi* rsf, void* buf, size_t count, off_t offset);

/* given a hash that defines a set of files, close our logical */
int redset_lofi_close(redset_lofi* rsf);

/* given a hash that defines a set of files, apply metadata recorded to each file */
int redset_lofi_apply_meta_mapped(kvtree* hash, const kvtree* map);

/* given a hash that defines a set of files, apply metadata recorded to each file */
int redset_lofi_apply_meta(kvtree* hash);

/* given a hash that defines a set of files, return list of files */
redset_filelist redset_lofi_filelist(const kvtree* hash);

#ifdef __cplusplus
} /* extern C */
#endif

#endif /* REDSET_LOFI_H */
