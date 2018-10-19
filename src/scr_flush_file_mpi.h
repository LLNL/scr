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

#ifndef SCR_FLUSH_FILE_MPI_H
#define SCR_FLUSH_FILE_MPI_H

/* returns true if the given dataset id needs to be flushed */
int scr_flush_file_need_flush(int id);

/* checks whether the specified dataset id is currently being flushed */
int scr_flush_file_is_flushing(int id);

/* removes entries in flush file for given dataset id */
int scr_flush_file_dataset_remove(int id);

/* adds a location for the specified dataset id to the flush file */
int scr_flush_file_location_set(int id, const char* location);

/* returns SCR_SUCCESS if specified dataset id is at specified location */
int scr_flush_file_location_test(int id, const char* location);

/* removes a location for the specified dataset id from the flush file */
int scr_flush_file_location_unset(int id, const char* location);

/* create an entry in the flush file for a dataset for scavenge,
 * including name, location, and flags */
int scr_flush_file_new_entry(int id, const char* name, const scr_dataset* dataset, const char* location, int ckpt, int output);

#endif
