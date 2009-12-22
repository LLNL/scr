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

#ifndef SCR_HALT_H
#define SCR_HALT_H

#include <stdio.h>

/*
=========================================
This file defines the data structure for a halt file.
=========================================
*/

/* data structure for halt file */
struct scr_haltdata
{
  char exit_reason[SCR_MAX_FILENAME];
  int  checkpoints_left;
  int  exit_before;
  int  exit_after;
  int  halt_seconds;
};

/* blank out a halt data structure */
int scr_halt_init(struct scr_haltdata* data);

/* given an opened file descriptor, read the fields for a halt file and fill in data */
int scr_halt_read_fd(int fd, struct scr_haltdata* data);

/* given an opened file descriptor, write halt fields to it, sync, and truncate */
int scr_halt_write_fd(int fd, struct scr_haltdata* data);

/* returns SCR_SUCCESS if halt file exists */
int scr_halt_exists(const char* file);

/* given the name of a halt file, read it and fill in data */
int scr_halt_read(const char* file, struct scr_haltdata* data);

/* read in halt file (which user may have changed via scr_halt), update internal data structure,
 * optionally decrement the checkpoints_left field, and write out halt file all while locked */
int scr_halt_sync_and_decrement(const char* file, struct scr_haltdata* data, int dec_count);

#endif
