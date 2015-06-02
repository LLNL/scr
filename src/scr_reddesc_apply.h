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

#ifndef SCR_REDDESC_APPLY_H
#define SCR_REDDESC_APPLY_H

#include "scr_hash.h"
#include "scr_meta.h"
#include "scr_filemap.h"
#include "scr_reddesc.h"

/* apply redundancy scheme to file and return number of bytes copied in bytes parameter */
int scr_reddesc_apply(scr_filemap* map, const scr_reddesc* c, int id, double* bytes);

#endif
