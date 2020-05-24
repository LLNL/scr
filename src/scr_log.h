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

/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
 */

#ifndef SCR_LOG_H
#define SCR_LOG_H

#include <sys/time.h>
#include <time.h>

/*
extern int scr_db_enable;
extern char* scr_db_host;
extern char* scr_db_user;
extern char* scr_db_pass;
extern char* scr_db_name;
*/

/* returns the current linux timestamp */
time_t scr_log_seconds(void);

/* initialize text file logging in prefix directory */
int scr_log_init_txt(const char* prefix);

/* initialize syslog logging */
int scr_log_init_syslog(void);

/* initialize the mysql database logging */
int scr_log_init_db(
  int debug,
  const char* host,
  const char* user,
  const char* pass,
  const char* name
);

/* initialize the logging */
int scr_log_init(const char* prefix);

/* shut down the logging */
int scr_log_finalize(void);

/* register a job with a username and prefix directory,
 * also take the hostname, jobid, and start time to capture
 * some state of the current run */
int scr_log_job(
  const char* username,
  const char* hostname,
  const char* jobid,
  const char* prefix,
  time_t start
);

/* log start time of current run along with
 * its number of procs and nodes */
int scr_log_run(
  time_t start,
  int procs,
  int nodes
);

/* log reason and time for halting current run */
int scr_log_halt(
  const char* reason
);

/* log an event */
int scr_log_event(
  const char* type,
  const char* note,
  const int* dset,
  const char* name,
  const time_t* start,
  const double* secs
);

/* log a transfer: copy / checkpoint / fetch / flush */
int scr_log_transfer(
  const char* type,
  const char* from,
  const char* to,
  const int* dset,
  const char* name,
  const time_t* start,
  const double* secs,
  const double* bytes,
  const int* files
);

#endif
