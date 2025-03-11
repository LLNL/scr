#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>
#include <stdarg.h>
#include <stdint.h>

// for stat
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <limits.h>
#include <unistd.h>

#include <fcntl.h>

/* compute crc32 */
#include <zlib.h>

#include "kvtree.h"
#include "kvtree_util.h"

#include "redset.h"
#include "redset_internal.h"

/* logically concatenate n opened files and read count bytes from this logical file into buf starting
 * from offset, pad with zero on end if missing data */
static int redset_read_pad_n(
  int n,                    /* number of files */
  const char** files,       /* filename for each file */
  int* fds,                 /* open file descriptor to each file */
  unsigned long* filesizes, /* file size of each file */
  char* buf,                /* buffer to read data into */
  unsigned long count,      /* number of bytes to read */
  unsigned long offset)     /* offset into logical file */
{
  int i = 0;
  size_t pos = 0;
  off_t nseek = 0;
  off_t nread = 0;

  /* pass through files until we find the one containing our offset */
  while (i < n && (nseek + filesizes[i]) <= offset) {
    nseek += filesizes[i];
    i++;
  }

  /* seek to the proper position in the current file */
  if (i < n) {
      pos = offset - nseek;
      nseek += pos;
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        redset_err("Failed to seek to byte %lu in %s @ %s:%d",
          pos, files[i], __FILE__, __LINE__
        );
        return REDSET_FAILURE;
      }
  }

  /* read data from files */
  while (nread < count && i < n) {
    /* assume we'll read the remainder of the current file */
    size_t num_to_read = filesizes[i] - pos;

    /* if we don't need to read the whole remainder of the file, adjust to the smaller amount */
    if (num_to_read > count - nread) {
      num_to_read = count - nread;
    }

    /* read data from file and add to the total read count */
    if (redset_read_attempt(files[i], fds[i], buf + nread, num_to_read) != num_to_read) {
      /* our read failed, return an error */
      return REDSET_FAILURE;
    }
    nread += num_to_read;

    /* advance to next file and seek to byte 0 */
    i++;
    if (i < n) {
      pos = 0;
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        redset_err("Failed to seek to byte %lu in %s @ %s:%d",
          pos, files[i], __FILE__, __LINE__
        );
        return REDSET_FAILURE;
      }
    }
  }

  /* if count is bigger than all of our file data, pad with zeros on the end */
  if (nread < count) {
    memset(buf + nread, 0, count - nread);
  }

  return REDSET_SUCCESS;
}

/* write to an array of open files with known filesizes treating them as one single large file */
static int redset_write_pad_n(
  int n,                    /* number of files */
  const char** files,       /* name of each file */
  int* fds,                 /* open file descriptor to each file */
  unsigned long* filesizes, /* file size of each file */
  char* buf,                /* buffer holding data to be written */
  unsigned long count,      /* number of bytes to write */
  unsigned long offset)     /* offset to write to in logical file */
{
  int i = 0;
  size_t pos = 0;
  off_t nseek  = 0;
  off_t nwrite = 0;

  /* pass through files until we find the one containing our offset */
  while (i < n && (nseek + filesizes[i]) <= offset) {
    nseek += filesizes[i];
    i++;
  }

  /* seek to the proper position in the current file */
  if (i < n) {
      pos = offset - nseek;
      nseek += pos;
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        redset_err("Failed to seek to byte %lu in %s @ %s:%d",
          pos, files[i], __FILE__, __LINE__
        );
        return REDSET_FAILURE;
      }
  }

  /* write data to files */
  while (nwrite < count && i < n) {
    /* assume we'll write the remainder of the current file */
    size_t num_to_write = filesizes[i] - pos;

    /* if we don't need to write the whole remainder of the file, adjust to the smaller amount */
    if (num_to_write > count - nwrite) {
      num_to_write = count - nwrite;
    }

    /* write data to file and add to the total write count */
    if (redset_write_attempt(files[i], fds[i], buf + nwrite, num_to_write) != num_to_write) {
      /* our write failed, return an error */
      return REDSET_FAILURE;
    }
    nwrite += num_to_write;

    /* advance to next file and seek to byte 0 */
    i++;
    if (i < n) {
      pos = 0;
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        redset_err("Failed to seek to byte %lu in %s @ %s:%d",
          pos, files[i], __FILE__, __LINE__
        );
        return REDSET_FAILURE;
      }
    }
  }

  /* if count is bigger than all of our file data, just throw the data away */
  if (nwrite < count) {
    /* NOTHING TO DO */
  }

  return REDSET_SUCCESS;
}

int redset_lofi_encode_kvtree(kvtree* hash, int num, const char** files)
{
  int rc = REDSET_SUCCESS;

  /* record total number of files we have */
  kvtree_set_kv_int(hash, "FILES", num);

  /* enter index, name, and size of each file */
  int i;
  kvtree* files_hash = kvtree_set(hash, "FILE", kvtree_new());
  for (i = 0; i < num; i++) {
    /* get file name of this file */
    const char* file = files[i];

    /* add entry for this file, including its index, name, and size */
    kvtree* file_hash = kvtree_setf(files_hash, kvtree_new(), "%d %s", i, file);

    /* record file meta data of this file */
    redset_meta_encode(file, file_hash);
  }

  return rc;
}

int redset_lofi_encode_map(kvtree* hash, int num, const char** src_files, const char** dst_files)
{
  int rc = REDSET_SUCCESS;

  /* record total number of files we have */
  kvtree_set_kv_int(hash, "FILES", num);

  /* enter index, name, and size of each file */
  int i;
  kvtree* files_hash = kvtree_set(hash, "FILE", kvtree_new());
  for (i = 0; i < num; i++) {
    /* get file name of this file */
    const char* src_file = src_files[i];
    const char* dst_file = dst_files[i];
    kvtree_util_set_str(files_hash, src_file, dst_file);
  }

  return rc;
}

int redset_lofi_check_mapped(const kvtree* hash, const kvtree* map)
{
  int rc = REDSET_SUCCESS;

  /* check that we got a hash to work with */
  if (hash == NULL) {
    return REDSET_FAILURE;
  }

  /* lookup number of files this process wrote */
  int num;
  if (kvtree_util_get_int(hash, "FILES", &num) != REDSET_SUCCESS) {
    /* number of files missing from hash */
    return REDSET_FAILURE;
  }

  /* assume we have our files */
  int have_my_files = 1;

  /* check that each of our data files exists and is the correct size */
  int i;
  kvtree* files_hash = kvtree_get(hash, "FILE");
  for (i = 0; i < num; i++) {
    /* get file name of this file */
    kvtree* index_hash = kvtree_getf(files_hash, "%d", i);
    kvtree_elem* elem = kvtree_elem_first(index_hash);
    const char* file = kvtree_elem_key(elem);

    /* lookup hash for this file */
    kvtree* file_hash = kvtree_getf(files_hash, "%d %s", i, file);
    if (file_hash == NULL) {
      /* failed to find file name recorded */
      have_my_files = 0;
      continue;
    }

    /* get name of file we're opening */
    char* file_name_mapped = (char*) file;
    if (map != NULL) {
      if (kvtree_util_get_str(map, file, &file_name_mapped) != KVTREE_SUCCESS) {
        /* given a map, but we failed to find this file in the map */
        redset_abort(-1, "Failed to find `%s' in map @ %s:%d",
          file, __FILE__, __LINE__
        );
      }
    }

    /* check that file exists */
    if (redset_file_exists(file_name_mapped) != REDSET_SUCCESS) {
      /* failed to find file */
      have_my_files = 0;
      continue;
    }

    /* get file size of this file */
    unsigned long file_size = redset_file_size(file_name_mapped);

    /* lookup expected file size and compare to actual size */
    unsigned long file_size_saved;
    if (kvtree_util_get_bytecount(file_hash, "SIZE", &file_size_saved) == KVTREE_SUCCESS) {
      if (file_size != file_size_saved) {
        /* file size does not match */
        have_my_files = 0;
        continue;
      }
    } else {
      /* file size not recorded */
      have_my_files = 0;
      continue;
    }
  }

  /* return failure if some file failed to check out */
  if (! have_my_files) {
    rc = REDSET_FAILURE;
  }

  return rc;
}

int redset_lofi_check(const kvtree* hash)
{
  int rc = redset_lofi_check_mapped(hash, NULL);
  return rc;
}

/* given a hash that defines a set of files, open our logical file for reading */
int redset_lofi_open_mapped(const kvtree* hash, const kvtree* map, int flags, mode_t mode, redset_lofi* rsf)
{
  int rc = REDSET_SUCCESS;

  /* bail out if we're not given a logical file structure to write to */
  if (rsf == NULL) {
    return REDSET_FAILURE;
  }

  /* initialize fields in caller's structure */
  rsf->numfiles  = 0;
  rsf->bytes     = 0;
  rsf->fds       = NULL;
  rsf->filenames = NULL;
  rsf->filesizes = NULL;

  /* lookup number of files this process wrote */
  int num_files = 0;
  if (kvtree_util_get_int(hash, "FILES", &num_files) != REDSET_SUCCESS) {
    redset_err("Failed to read number of files from hash @ %s:%d",
      __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* allocate arrays to hold file descriptors, filenames, and filesizes for each of our files */
  int* fds                 = (int*)           REDSET_MALLOC(num_files * sizeof(int));
  const char** filenames   = (const char**)   REDSET_MALLOC(num_files * sizeof(char*));
  unsigned long* filesizes = (unsigned long*) REDSET_MALLOC(num_files * sizeof(unsigned long));

  unsigned long bytes = 0;

  /* open each of our files */
  int i;
  kvtree* files_hash = kvtree_get(hash, "FILE");
  for (i = 0; i < num_files; i++) {
    /* get file name of this file */
    kvtree* index_hash = kvtree_getf(files_hash, "%d", i);
    kvtree_elem* elem = kvtree_elem_first(index_hash);
    const char* file_name = kvtree_elem_key(elem);

    /* lookup hash for this file */
    kvtree* file_hash = kvtree_getf(files_hash, "%d %s", i, file_name);

    /* get name of file we're opening */
    char* file_name_mapped = (char*) file_name;
    if (map != NULL) {
      if (kvtree_util_get_str(map, file_name, &file_name_mapped) != KVTREE_SUCCESS) {
        /* given a map, but we failed to find this file in the map */
        redset_abort(-1, "Failed to find `%s' in map @ %s:%d",
          file_name, __FILE__, __LINE__
        );
      }
    }

    /* copy the full filename */
    filenames[i] = strdup(file_name_mapped);
    if (filenames[i] == NULL) {
      redset_abort(-1, "Failed to copy filename %s @ %s:%d",
        file_name, __FILE__, __LINE__
      );
    }

    /* lookup the filesize */
    if (kvtree_util_get_bytecount(file_hash, "SIZE", &filesizes[i]) != REDSET_SUCCESS) {
      redset_abort(-1, "Failed to read file size for file %s @ %s:%d",
        file_name, __FILE__, __LINE__
      );
    }

    /* sum up bytes in our of our files */
    bytes += filesizes[i];

    /* open the file for reading */
    if ((mode & O_RDONLY) == O_RDONLY) {
      fds[i] = redset_open(file_name_mapped, flags);
      if (fds[i] < 0) {
        redset_abort(-1, "Opening file for reading: redset_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
          file_name_mapped, errno, strerror(errno), __FILE__, __LINE__
        );
      }
    } else {
      fds[i] = redset_open(file_name_mapped, flags, mode);
      if (fds[i] < 0) {
        redset_abort(-1, "Opening file for writing: redset_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
          file_name_mapped, errno, strerror(errno), __FILE__, __LINE__
        );
      }
    }
  }

  /* success if we made it here, copy everything to output struct */
  rsf->numfiles  = num_files;
  rsf->bytes     = bytes;
  rsf->fds       = fds;
  rsf->filenames = filenames;
  rsf->filesizes = filesizes;

  return rc;
}

/* given a hash that defines a set of files, open our logical file for reading */
int redset_lofi_open(const kvtree* hash, int flags, mode_t mode, redset_lofi* rsf)
{
  int rc = redset_lofi_open_mapped(hash, NULL, flags, mode, rsf);
  return rc;
}

/* return file size of our logical file */
unsigned long redset_lofi_bytes(redset_lofi* rsf)
{
  if (rsf == NULL) {
    return 0;
  }
  return rsf->bytes;
}

/* read from logical file */
int redset_lofi_pread(redset_lofi* rsf, void* buf, size_t count, off_t offset)
{
  if (rsf == NULL) {
    return REDSET_FAILURE;
  }

  int rc = redset_read_pad_n(
    rsf->numfiles, rsf->filenames, rsf->fds, rsf->filesizes,
    buf, count, offset
  );

  return rc;
}

/* write to logical file */
int redset_lofi_pwrite(redset_lofi* rsf, void* buf, size_t count, off_t offset)
{
  if (rsf == NULL) {
    return REDSET_FAILURE;
  }

  int rc = redset_write_pad_n(
    rsf->numfiles, rsf->filenames, rsf->fds, rsf->filesizes,
    buf, count, offset
  );

  return rc;
}

/* given a hash that defines a set of files, close our logical */
int redset_lofi_close(redset_lofi* rsf)
{
  if (rsf == NULL) {
    return REDSET_FAILURE;
  }

  int rc = REDSET_SUCCESS;

  /* close each file */
  int i;
  for (i = 0; i < rsf->numfiles; i++) {
    int fd = rsf->fds[i];
    const char* file = rsf->filenames[i];
    if (redset_close(file, fd) != REDSET_SUCCESS) {
      redset_err("Error closing file: redset_close(%s) errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = REDSET_FAILURE;
    }

    /* free file name strdup'd during open */
    redset_free(&rsf->filenames[i]);
  }

  /* free memory allocated for file descriptors, names, and sizes */
  redset_free(&rsf->fds);
  redset_free(&rsf->filenames);
  redset_free(&rsf->filesizes);

  return rc;
}

/* given a hash that defines a set of files, apply metadata recorded to each file */
int redset_lofi_apply_meta_mapped(kvtree* hash, const kvtree* map)
{
  if (hash == NULL) {
    return REDSET_FAILURE;
  }

  int rc = REDSET_SUCCESS;

  /* lookup number of files this process wrote */
  int num_files = 0;
  if (kvtree_util_get_int(hash, "FILES", &num_files) != REDSET_SUCCESS) {
    redset_err("Failed to read number of files from hash @ %s:%d",
      __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* we've written data for all files */
  int i;
  kvtree* files_hash = kvtree_get(hash, "FILE");
  for (i = 0; i < num_files; i++) {
    /* get file name of this file */
    kvtree* index_hash = kvtree_getf(files_hash, "%d", i);
    kvtree_elem* elem = kvtree_elem_first(index_hash);
    const char* file_name = kvtree_elem_key(elem);

    /* lookup hash for this file */
    kvtree* file_hash = kvtree_getf(files_hash, "%d %s", i, file_name);

    /* get name of file we're opening */
    char* file_name_mapped = (char*) file_name;
    if (map != NULL) {
      if (kvtree_util_get_str(map, file_name, &file_name_mapped) != KVTREE_SUCCESS) {
        /* given a map, but we failed to find this file in the map */
        redset_abort(-1, "Failed to find `%s' in map @ %s:%d",
          file_name, __FILE__, __LINE__
        );
      }
    }

    /* set metadata properties on rebuilt file */
    redset_meta_apply(file_name_mapped, file_hash);
  }

  return rc;
}

/* given a hash that defines a set of files, apply metadata recorded to each file */
int redset_lofi_apply_meta(kvtree* hash)
{
  int rc = redset_lofi_apply_meta_mapped(hash, NULL);
  return rc;
}

/* given a hash that defines a set of files, return list of files */
redset_filelist redset_lofi_filelist(const kvtree* hash)
{
  /* lookup number of files this process wrote */
  int num_files = 0;
  if (kvtree_util_get_int(hash, "FILES", &num_files) != REDSET_SUCCESS) {
    redset_abort(-1, "Failed to read number of files from hash @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate arrays to hold filenames */
  const char** filenames = (const char**) REDSET_MALLOC(num_files * sizeof(char*));

  /* open each of our files */
  int i;
  kvtree* files_hash = kvtree_get(hash, "FILE");
  for (i = 0; i < num_files; i++) {
    /* get file name of this file */
    kvtree* index_hash = kvtree_getf(files_hash, "%d", i);
    kvtree_elem* elem = kvtree_elem_first(index_hash);
    const char* file_name = kvtree_elem_key(elem);

    /* copy the full filename */
    filenames[i] = strdup(file_name);
    if (filenames[i] == NULL) {
      redset_abort(-1, "Failed to copy filename %s @ %s:%d",
        file_name, __FILE__, __LINE__
      );
    }
  }

  redset_list* list = (redset_list*) REDSET_MALLOC(sizeof(redset_list));
  list->count = num_files;
  list->files = filenames;

  return list;
}
