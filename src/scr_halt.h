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
#include "scr_hash.h"

/*
=========================================
This file defines the data structure for a halt file.
=========================================
*/

#define SCR_HALT_KEY_EXIT_REASON ("ExitReason")
#define SCR_HALT_KEY_SECONDS     ("HaltSeconds")
#define SCR_HALT_KEY_EXIT_BEFORE ("ExitBefore")
#define SCR_HALT_KEY_EXIT_AFTER  ("ExitAfter")
#define SCR_HALT_KEY_CHECKPOINTS ("CheckpointsLeft")

/* given the name of a halt file, read it and fill in data */
int scr_halt_read(const scr_path* file, scr_hash* hash);

/* read in halt file (which user may have changed via scr_halt), update internal data structure,
 * optionally decrement the checkpoints_left field, and write out halt file all while locked */
int scr_halt_sync_and_decrement(const scr_path* file, scr_hash* hash, int dec_count);

#endif
