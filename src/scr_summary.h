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

#ifndef SCR_SUMMARY_H
#define SCR_SUMMARY_H

#include "spath.h"
#include "kvtree.h"
#include "scr_dataset.h"

/* read in the summary file from dir */
int scr_summary_read(const spath* dir, kvtree* summary_hash);

#endif
