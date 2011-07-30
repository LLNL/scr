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

#include <stdlib.h>
#include <stdint.h>

/* given a string, convert it to a double and write that value to val */
int scr_atod(char* str, double* val);

/* converts string like 10mb to unsigned long long integer value of 10*1024*1024 */
int scr_abtoull(char* str, unsigned long long* val);

/* allocates a block of memory and aligns it to specified alignment */
void* scr_align_malloc(size_t size, size_t align);

/* frees a blocked allocated with a call to scr_align_malloc */
void scr_align_free(void* buf);

/* returns the current linux timestamp (in microseconds) */
int64_t scr_time_usecs();


/* pack an unsigned 16 bit value to specified buffer in network order */
int scr_pack_uint16_t(void* buf, size_t buf_size, size_t* buf_pos, uint16_t val);

/* pack an unsigned 32 bit value to specified buffer in network order */
int scr_pack_uint32_t(void* buf, size_t buf_size, size_t* buf_pos, uint32_t val);

/* pack an unsigned 64 bit value to specified buffer in network order */
int scr_pack_uint64_t(void* buf, size_t buf_size, size_t* buf_pos, uint64_t val);

/* unpack an unsigned 16 bit value to specified buffer in network order */
int scr_unpack_uint16_t(void* buf, size_t buf_size, size_t* buf_pos, uint16_t* val);

/* unpack an unsigned 32 bit value to specified buffer in network order */
int scr_unpack_uint32_t(void* buf, size_t buf_size, size_t* buf_pos, uint32_t* val);

/* unpack an unsigned 64 bit value to specified buffer in network order */
int scr_unpack_uint64_t(void* buf, size_t buf_size, size_t* buf_pos, uint64_t* val);

#endif
