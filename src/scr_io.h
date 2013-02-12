/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#ifndef SCR_IO_H
#define SCR_IO_H

#include <config.h>
#include <stdarg.h>
#include <sys/types.h>

/* compute crc32 */
#include <zlib.h>

#ifndef SCR_MAX_LINE
#define SCR_MAX_LINE (1024)
#endif

/* adds byte swapping routines */
#include "endian.h"

#ifdef HAVE_BYTESWAP_H
#include "byteswap.h"
#else
#include "scr_byteswap.h"
#endif

#if __BYTE_ORDER == __LITTLE_ENDIAN 
#ifdef HAVE_BYTESWAP_H
# define scr_ntoh16(x) bswap_16(x)
# define scr_ntoh32(x) bswap_32(x)
# define scr_ntoh64(x) bswap_64(x)
# define scr_hton16(x) bswap_16(x)
# define scr_hton32(x) bswap_32(x)
# define scr_hton64(x) bswap_64(x)
#else
# define scr_ntoh16(x) scr_bswap_16(x)
# define scr_ntoh32(x) scr_bswap_32(x)
# define scr_ntoh64(x) scr_bswap_64(x)
# define scr_hton16(x) scr_bswap_16(x)
# define scr_hton32(x) scr_bswap_32(x)
# define scr_hton64(x) scr_bswap_64(x)
#endif
#else
# define scr_ntoh16(x) (x)
# define scr_ntoh32(x) (x)
# define scr_ntoh64(x) (x)
# define scr_hton16(x) (x)
# define scr_hton32(x) (x)
# define scr_hton64(x) (x)
#endif

/*
=========================================
Basic File I/O
=========================================
*/

/* returns user's current mode as determine by his umask */
mode_t scr_getmode(int read, int write, int execute);

/* open file with specified flags and mode, retry open a few times on failure */
int scr_open(const char* file, int flags, ...);

/* close file with an fsync */
int scr_close(const char* file, int fd);

/* get and release file locks */
int scr_file_lock_read(const char* file, int fd);
int scr_file_lock_write(const char* file, int fd);
int scr_file_unlock(const char* file, int fd);

/* opens specified file and waits on for an exclusive lock before returning the file descriptor */
int scr_open_with_lock(const char* file, int flags, mode_t mode);

/* unlocks the specified file descriptor and then closes the file */
int scr_close_with_unlock(const char* file, int fd);

/* seek file descriptor to specified position */
int scr_lseek(const char* file, int fd, off_t pos, int whence);

/* reliable read from opened file descriptor (retries, if necessary, until hard error) */
ssize_t scr_read(const char* file, int fd, void* buf, size_t size);

/* reliable write to opened file descriptor (retries, if necessary, until hard error) */
ssize_t scr_write(const char* file, int fd, const void* buf, size_t size);

/* make a good attempt to read from file (retries, if necessary, return error if fail) */
ssize_t scr_read_attempt(const char* file, int fd, void* buf, size_t size);

/* make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t scr_write_attempt(const char* file, int fd, const void* buf, size_t size);

/* read line from file into buf with given size */
ssize_t scr_read_line(const char* file, int fd, char* buf, size_t size);

/* write a formatted string to specified file descriptor */
ssize_t scr_writef(const char* file, int fd, const char* format, ...);

/* logically concatenate n opened files and read count bytes from this logical file into buf starting
 * from offset, pad with zero on end if missing data */
int scr_read_pad_n(
  int n,
  char** files,
  int* fds,
  char* buf,
  unsigned long count,
  unsigned long offset,
  unsigned long* filesizes
);

/* write to an array of open files with known filesizes and treat them as one single large file */
int scr_write_pad_n(
  int n,
  char** files,
  int* fds,
  char* buf,
  unsigned long count,
  unsigned long offset,
  unsigned long* filesizes
);

/* given a filename, return number of bytes in file */
unsigned long scr_file_size(const char* file);

/* tests whether the file or directory exists */
int scr_file_exists(const char* file);

/* tests whether the file or directory is readable */
int scr_file_is_readable(const char* file);

/* tests whether the file or directory is writeable */
int scr_file_is_writeable(const char* file);

/* delete a file */
int scr_file_unlink(const char* file);

/* opens, reads, and computes the crc32 value for the given filename */
int scr_crc32(const char* filename, uLong* crc);

/*
=========================================
Directory functions
=========================================
*/

/* recursively create directory and subdirectories */
int scr_mkdir(const char* dir, mode_t mode);

/* remove directory */
int scr_rmdir(const char* dir);

/* write current working directory to buf */
int scr_getcwd(char* buf, size_t size);

/*
=========================================
File Copy Functions
=========================================
*/

int scr_file_copy(
  const char* src_file,
  const char* dst_file,
  unsigned long buf_size,
  uLong* crc
);

/*
=========================================
File compression functions
=========================================
*/

/* compress the specified file using blocks of size block_size and store as file_dst */
int scr_compress_in_place(const char* file_src, const char* file_dst, unsigned long block_size, int level);

/* uncompress the specified file and store as file_dst */
int scr_uncompress_in_place(const char* file_src, const char* file_dst);

/* compress the specified file using blocks of size block_size and store as file_dst */
int scr_compress(const char* file_src, const char* file_dst, unsigned long block_size, int level);

/* uncompress the specified file and store as file_dst */
int scr_uncompress(const char* file_src, const char* file_dst);

#endif
