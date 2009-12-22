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

#include <sys/types.h>

/*
=========================================
Basic File I/O
=========================================
*/

/* open file with specified flags and mode, retry open a few times on failure */
int scr_open(const char* file, int flags, ...);

/* close file with an fsync */
int scr_close(int fd);

/* reliable read from file descriptor (retries, if necessary, until hard error) */
int scr_read(int fd, void* buf, size_t size);

/* reliable write to file descriptor (retries, if necessary, until hard error) */
int scr_write(int fd, const void* buf, size_t size);

/* read count bytes from fd into buf starting from offset, pad with zero if missing data */
int scr_read_pad(int fd, char* buf, unsigned long count, unsigned long offset, unsigned long filesize);

/* like scr_read_pad, but this takes an array of open files and treats them as one single large file */
int scr_read_pad_n(int n, int* fds, char* buf, unsigned long count, unsigned long offset, unsigned long* filesizes);

/* write to an array of open files with known filesizes and treat them as one single large file */
int scr_write_pad_n(int n, int* fds, char* buf, unsigned long count, unsigned long offset, unsigned long* filesizes);

/* given a filename, return number of bytes in file */
unsigned long scr_filesize(const char* file);

/* split path and filename from fullpath on the rightmost '/'
   assumes all filename if no '/' is found */
int scr_split_path (const char* file, char* path, char* filename);

/* combine path and filename into a fullpath in file */
int scr_build_path (char* file, const char* path, const char* filename);

/* recursively create directory and subdirectories */
int scr_mkdir(const char* dir, mode_t mode);

#endif
