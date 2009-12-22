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

#ifndef SCR_ERR_H
#define SCR_ERR_H

/*
=========================================
Error and Debug Messages
=========================================
*/

/* print message to stderr */
void scr_err(const char *fmt, ...);

/* print message to stdout if scr_debug is set and it is >= level */
void scr_dbg(int level, const char *fmt, ...);

/* print abort message and kill run */
void scr_abort(int rc, const char *fmt, ...);

#endif
