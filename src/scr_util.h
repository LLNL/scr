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

#ifndef SCR_UTIL_H
#define SCR_UTIL_H

/* given a string, convert it to a double and write that value to val */
int scr_atod(char* str, double* val);

/* converts string like 10mb to unsigned long long integer value of 10*1024*1024 */
int scr_abtoull(char* str, unsigned long long* val);

/* allocates a block of memory and aligns it to specified alignment */
void* scr_align_malloc(size_t size, size_t align);

/* frees a blocked allocated with a call to scr_align_malloc */
void scr_align_free(void* buf);

#endif
