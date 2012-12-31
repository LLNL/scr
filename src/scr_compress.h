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

#ifndef SCR_COMPRESS_H
#define SCR_COMPRESS_H

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

#endif /* SCR_COMPRESS_H */
