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

#ifndef SCR_PREFIX_H
#define SCR_PREFIX_H

/* delete named dataset from the prefix directory */
int scr_prefix_delete(int id, const char* name);

/* keep a sliding window of checkpoints in the prefix directory,
 * delete any pure checkpoints that fall outside of the window
 * defined by the given dataset id and the window width,
 * excludes checkpoints that are marked as output */
int scr_prefix_delete_sliding(int id, int window);

/* delete all datasets listed in the index file,
 * both checkpoint and output */
int scr_prefix_delete_all(void);

#endif /* SCR_PREFIX_H */
