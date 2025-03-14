#ifndef SHUFFILE_IO_H
#define SHUFFILE_IO_H

#include <config.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

/* compute crc32 */
#include <zlib.h>

#ifndef SHUFFILE_MAX_LINE
#define SHUFFILE_MAX_LINE (1024)
#endif

/** \file shuffile_io.h
 *  \ingroup shuffile
 *  \brief some I/O utilities */

/********************************************************/
/** \name Basic File I/O */
///@{

/** returns user's current mode as determine by his umask */
mode_t shuffile_getmode(int read, int write, int execute);

/** open file with specified flags and mode, retry open a few times on failure */
int shuffile_open(const char* file, int flags, ...);

/** close file with an fsync */
int shuffile_close(const char* file, int fd);

/** get and release file locks */
int shuffile_file_lock_read(const char* file, int fd);
int shuffile_file_lock_write(const char* file, int fd);
int shuffile_file_unlock(const char* file, int fd);

/** opens specified file and waits on for an exclusive lock before returning the file descriptor */
int shuffile_open_with_lock(const char* file, int flags, mode_t mode);

/** unlocks the specified file descriptor and then closes the file */
int shuffile_close_with_unlock(const char* file, int fd);

/** seek file descriptor to specified position */
int shuffile_lseek(const char* file, int fd, off_t pos, int whence);

/** reliable read from opened file descriptor (retries, if necessary, until hard error) */
ssize_t shuffile_read(const char* file, int fd, void* buf, size_t size);

/** reliable write to opened file descriptor (retries, if necessary, until hard error) */
ssize_t shuffile_write(const char* file, int fd, const void* buf, size_t size);

/** make a good attempt to read from file (retries, if necessary, return error if fail) */
ssize_t shuffile_read_attempt(const char* file, int fd, void* buf, size_t size);

/** make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t shuffile_write_attempt(const char* file, int fd, const void* buf, size_t size);

/** read line from file into buf with given size */
ssize_t shuffile_read_line(const char* file, int fd, char* buf, size_t size);

/** write a formatted string to specified file descriptor */
ssize_t shuffile_writef(const char* file, int fd, const char* format, ...);

/** logically concatenate n opened files and read count bytes from this logical file into buf starting
 * from offset, pad with zero on end if missing data */
int shuffile_read_pad_n(
  int n,
  const char** files,
  int* fds,
  char* buf,
  unsigned long count,
  unsigned long offset,
  unsigned long* filesizes
);

/** write to an array of open files with known filesizes and treat them as one single large file */
int shuffile_write_pad_n(
  int n,
  const char** files,
  int* fds,
  char* buf,
  unsigned long count,
  unsigned long offset,
  unsigned long* filesizes
);

/** given a filename, return stat info */
int shuffile_stat(const char* file, struct stat* statbuf);

/** given a filename, return number of bytes in file */
unsigned long shuffile_file_size(const char* file);

/** tests whether the file or directory exists */
int shuffile_file_exists(const char* file);

/** tests whether the file or directory is readable */
int shuffile_file_is_readable(const char* file);

/** tests whether the file or directory is writeable */
int shuffile_file_is_writeable(const char* file);

/** delete a file */
int shuffile_file_unlink(const char* file);

/** opens, reads, and computes the crc32 value for the given filename */
int shuffile_crc32(const char* filename, uLong* crc);
///@}

/********************************************************/
/** \name Directory functions */
///@{

/** recursively create directory and subdirectories */
int shuffile_mkdir(const char* dir, mode_t mode);

/** remove directory */
int shuffile_rmdir(const char* dir);

/** write current working directory to buf */
int shuffile_getcwd(char* buf, size_t size);
///@}

/********************************************************/
/** \name File Copy Functions */
///@{

int shuffile_file_copy(
  const char* src_file,
  const char* dst_file,
  unsigned long buf_size,
  uLong* crc
);
///@}

/********************************************************/
/** \name File compression functions */
///@{

/** compress the specified file using blocks of size block_size and store as file_dst */
int shuffile_compress_in_place(const char* file_src, const char* file_dst, unsigned long block_size, int level);

/** uncompress the specified file and store as file_dst */
int shuffile_uncompress_in_place(const char* file_src, const char* file_dst);

/** compress the specified file using blocks of size block_size and store as file_dst */
int shuffile_compress(const char* file_src, const char* file_dst, unsigned long block_size, int level);

/** uncompress the specified file and store as file_dst */
int shuffile_uncompress(const char* file_src, const char* file_dst);
///@}

#endif
