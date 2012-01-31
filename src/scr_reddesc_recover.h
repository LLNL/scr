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

#ifndef SCR_REDDESC_RECOVER_H
#define SCR_REDDESC_RECOVER_H

#include "scr_reddesc.h"
#include "scr_filemap.h"

/* rebuilds files for specified dataset id using specified redundancy descriptor,
 * adds them to filemap, and returns SCR_SUCCESS if all processes succeeded */
int scr_reddesc_recover(scr_filemap* map, const scr_reddesc* c, int id);

#endif
