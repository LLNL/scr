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

#include "scr_hash.h"
#include "scr_dataset.h"

/* read in the summary file from dir */
int scr_summary_read(const scr_path* dir, scr_hash* summary_hash);

/* write out the summary file to dir */
int scr_summary_write(const scr_path* dir, const scr_dataset* dataset, int all_complete, scr_hash* data);

#endif
