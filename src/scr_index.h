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

/* Implements an interface to read and write index files. */

#ifndef SCR_HALT_H
#define SCR_HALT_H

#include <stdio.h>
#include "scr_hash.h"

/* read the index file from given directory and merge its contents into the given hash */
int scr_index_read(const char* dir, struct scr_hash* index);

/* overwrite the contents of the index file in given directory with given hash */
int scr_index_write(const char* dir, struct scr_hash* index);

/* add given checkpoint id and directory name to given hash */
int scr_index_add_checkpoint_dir(struct scr_hash* index, int checkpoint_id, const char* name);

/* write completeness code (0 or 1) for given checkpoint id and directory in given hash */
int scr_index_mark_completeness(struct scr_hash* index, int checkpoint_id, const char* name, int complete);

/* record fetch event for given checkpoint id and directory in given hash */
int scr_index_mark_fetched(struct scr_hash* index, int checkpoint_id, const char* name);

/* record failed fetch event for given checkpoint id and directory in given hash */
int scr_index_mark_failed(struct scr_hash* index, int checkpoint_id, const char* name);

/* lookup the checkpoint id corresponding to the given checkpoint directory name in given hash
 * (assumes a directory maps to a single checkpoint id) */
int scr_index_get_checkpoint_id_by_dir(const struct scr_hash* index, const char* name, int* checkpoint_id);

/* lookup the most recent complete checkpoint id and directory whose id is less than earlier_than
 * setting earlier_than = -1 disables this filter */
int scr_index_most_recent_complete(const struct scr_hash* index, int earlier_than, int* checkpoint_id, char* name);

#endif
