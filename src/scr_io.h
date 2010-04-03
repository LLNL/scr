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

#include <stdarg.h>
#include <sys/types.h>

/* compute crc32 */
#include <zlib.h>

#ifndef SCR_MAX_LINE
#define SCR_MAX_LINE (1024)
#endif

/*
=========================================
Basic File I/O
=========================================
*/

/* open file with specified flags and mode, retry open a few times on failure */
int scr_open(const char* file, int flags, ...);

/* close file with an fsync */
int scr_close(const char* file, int fd);

/* opens specified file and waits on for an exclusive lock before returning the file descriptor */
int scr_open_with_lock(const char* file, int flags, mode_t mode);

/* unlocks the specified file descriptor and then closes the file */
int scr_close_with_unlock(const char* file, int fd);

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t scr_read(int fd, void* buf, size_t size);

/* reliable write to file descriptor (retries, if necessary, until hard error) */
ssize_t scr_write(int fd, const void* buf, size_t size);

/* make a good attempt to read from file (retries, if necessary, return error if fail) */
ssize_t scr_read_attempt(const char* file, int fd, void* buf, size_t size);

/* make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t scr_write_attempt(const char* file, int fd, const void* buf, size_t size);

/* read line from file into buf with given size */
ssize_t scr_read_line(const char* file, int fd, char* buf, size_t size);

/* write a formatted string to specified file descriptor */
ssize_t scr_writef(const char* file, int fd, const char* format, ...);

/* read count bytes from fd into buf starting from offset, pad with zero if missing data */
int scr_read_pad(int fd, char* buf, unsigned long count, unsigned long offset, unsigned long filesize);

/* like scr_read_pad, but this takes an array of open files and treats them as one single large file */
int scr_read_pad_n(int n, char** files, int* fds,
                   char* buf, unsigned long count, unsigned long offset, unsigned long* filesizes);

/* write to an array of open files with known filesizes and treat them as one single large file */
int scr_write_pad_n(int n, char** files, int* fds,
                    char* buf, unsigned long count, unsigned long offset, unsigned long* filesizes);

/* given a filename, return number of bytes in file */
unsigned long scr_filesize(const char* file);

/* tests whether the file exists */
int scr_file_exists(const char* file);

/* opens, reads, and computes the crc32 value for the given filename */
int scr_crc32(const char* filename, uLong* crc);

/* split path and filename from fullpath on the rightmost '/'
   assumes all filename if no '/' is found */
int scr_split_path (const char* file, char* path, char* filename);

/* combine path and filename into a fullpath in file */
int scr_build_path (char* file, const char* path, const char* filename);

/* recursively create directory and subdirectories */
int scr_mkdir(const char* dir, mode_t mode);

/*
=========================================
File Copy Functions
=========================================
*/

int scr_copy_to(const char* src, const char* dst_dir, unsigned long buf_size, char* dst, uLong* crc);

/*
=========================================
Timing
=========================================
*/

/* returns the current linux timestamp (secs + usecs since epoch) as a double */
double scr_seconds();

#endif
