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
time_t scr_log_seconds();

/* initialize the logging */
int scr_log_init();

/* shut down the logging */
int scr_log_finalize();

/* given a username, a jobname, and a start time, lookup (or create) the id for this job */
int scr_log_job(char* username, char* jobname, time_t start);

/* log start time of current run */
int scr_log_run(time_t start);

/* log reason and time for halting current run */
int scr_log_halt(char* reason, int* ckpt);

/* log an event */
int scr_log_event(char* type, char* note, int* ckpt, time_t* start, double* secs);

/* log a transfer: copy / checkpoint / fetch / flush */
int scr_log_transfer(char* type, char* from, char* to, int* ckpt_id, time_t* start, double* secs, double* bytes);

#endif
