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

/* Implements some common tasks for SCR operations on hashes */

#ifndef SCR_HASH_UTIL_H
#define SCR_HASH_UTIL_H

#include "scr_hash.h"

/* compute crc32 */
#include <zlib.h>

int scr_hash_util_set_bytecount(scr_hash* hash, const char* key, unsigned long count);

int scr_hash_util_set_crc32(scr_hash* hash, const char* key, uLong crc);


int scr_hash_util_get_bytecount(const scr_hash* hash, const char* key, unsigned long* val);

int scr_hash_util_get_crc32(const scr_hash* hash, const char* key, uLong* val);


int scr_hash_util_get_int(const scr_hash* hash, const char* key, int* value);

int scr_hash_util_get_unsigned_long(const scr_hash* hash, const char* key, unsigned long* value);

#endif
