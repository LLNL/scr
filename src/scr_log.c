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

/* Implements a logging interface for SCR events and file transfer operations */

#include "scr_conf.h"
#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_util.h"
#include "scr_param.h"
#include "scr_log.h"
#include "scr_hash.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <strings.h>

/* gettimeofday */
#include <sys/time.h>

/* localtime, asctime */
#include <time.h>

#ifdef HAVE_LIBMYSQLCLIENT
#include <mysql/mysql.h>
#endif

static int scr_db_enable = 0;    /* whether to log event in SCR log database */
static int scr_db_debug  = 0;    /* database debug level */
static char* scr_db_host = NULL; /* hostname or IP running DB server */
static char* scr_db_user = NULL; /* username to use to connect to DB server */
static char* scr_db_pass = NULL; /* password to use to connect to DB server */
static char* scr_db_name = NULL; /* database name to connect to */

/*
=========================================
MySQL functions
=========================================
*/

#ifdef HAVE_LIBMYSQLCLIENT
static MYSQL scr_mysql;
#endif

static unsigned long scr_db_jobid = 0;       /* caches the jobid for the current job */
#ifdef HAVE_LIBMYSQLCLIENT
static scr_hash* scr_db_types = NULL; /* caches type string to type id lookups */
#endif

/* connects to the SCR log database */
int scr_mysql_connect()
{
#ifdef HAVE_LIBMYSQLCLIENT
  /* TODO: read in connection parameters using scr_get_param calls */

  /* create our type string to id cache */
  scr_db_types = scr_hash_new();
  if (scr_db_types == NULL) {
    scr_err("Failed to create a hash to cache type string to id lookups @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* initialize our database structure */
  mysql_init(&scr_mysql);

  /* connect to the database */
  if (!mysql_real_connect(&scr_mysql, scr_db_host, scr_db_user, scr_db_pass, scr_db_name, 0, NULL, 0)) {
    scr_err("Failed to connect to SCR log database %s on host %s @ %s:%d",
            scr_db_name, scr_db_host, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

#endif
  return SCR_SUCCESS;
}

/* disconnects from SCR log database */
int scr_mysql_disconnect()
{
#ifdef HAVE_LIBMYSQLCLIENT
  /* free our type string to id cache */
  scr_hash_delete(&scr_db_types);

  mysql_close(&scr_mysql);
#endif
  return SCR_SUCCESS;
}

char* scr_mysql_quote_string(const char* value)
{
#ifdef HAVE_LIBMYSQLCLIENT
  if (value != NULL) {
    int n = strlen(value);
    char* q = (char*) malloc(2*n+1+2);
    if (q != NULL) {
      q[0] = '\'';
      mysql_real_escape_string(&scr_mysql, &q[1], value, n);
      n = strlen(q);
      q[n] = '\'';
      q[n+1] = '\0';
      return q;
    } else {
      scr_err("Failed to allocate buffer space to encode string %s @ %s:%d",
              value, __FILE__, __LINE__
      );
      return NULL;
    }
  } else {
    return strdup("NULL");
  }
#else
  return NULL;
#endif
}

/* given a number of seconds since the epoch, fill in a string for mysql datetime */
char* scr_mysql_quote_seconds(const time_t* value)
{
#ifdef HAVE_LIBMYSQLCLIENT
  if (value != NULL) {
    char buf[30];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", localtime(value));
    return scr_mysql_quote_string(buf);
  } else {
    return scr_mysql_quote_string((char*) value);
  }
#else
  return NULL;
#endif
}

char* scr_mysql_quote_int(const int* value)
{
#ifdef HAVE_LIBMYSQLCLIENT
  if (value != NULL) {
    char buf[1024];
    int n = snprintf(buf, sizeof(buf), "%d", *value);

    /* check that we were able to print the value ok */
    if (n >= sizeof(buf)) {
      scr_err("Insufficient buffer space (%lu bytes) to encode value (%lu needed) @ %s:%d",
              sizeof(buf), n, __FILE__, __LINE__
      );
      return NULL;
    }

    return scr_mysql_quote_string(buf);
  } else {
    return scr_mysql_quote_string((char*) value);
  }
#else
  return NULL;
#endif
}

char* scr_mysql_quote_double(const double* value)
{
#ifdef HAVE_LIBMYSQLCLIENT
  if (value != NULL) {
    char buf[1024];
    int n = snprintf(buf, sizeof(buf), "%f", *value);

    /* check that we were able to print the value ok */
    if (n >= sizeof(buf)) {
      scr_err("Insufficient buffer space (%lu bytes) to encode value (%lu needed) @ %s:%d",
              sizeof(buf), n, __FILE__, __LINE__
      );
      return NULL;
    }

    return scr_mysql_quote_string(buf);
  } else {
    return scr_mysql_quote_string((char*) value);
  }
#else
  return NULL;
#endif
}

/* lookup name in table and return id if found */
int scr_mysql_read_id(const char* table, const char* name, unsigned long* id)
{
#ifdef HAVE_LIBMYSQLCLIENT
  /* escape parameter */
  char* qname = scr_mysql_quote_string(name);

  /* check that we got valid strings for each of our parameters */
  if (qname == NULL) {
    scr_err("Failed to escape and quote one or more arguments @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* construct the query */
  char query[1024];
  int n = snprintf(query, sizeof(query),
    "SELECT * FROM `%s` WHERE `name` = %s ;",
    table, qname
  );

  /* free the strings as they are now encoded into the query */
  scr_free(&qname);

  /* check that we were able to construct the query ok */
  if (n >= sizeof(query)) {
    scr_err("Insufficient buffer space (%lu bytes) to build query (%lu bytes) @ %s:%d",
            sizeof(query), n, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* execute the query */
  if (scr_db_debug >= 1) {
    scr_dbg(0, "%s", query);
  }
  if (mysql_real_query(&scr_mysql, query, (unsigned int) strlen(query))) {
    scr_err("Select failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* prepare the result set to be used */
  MYSQL_RES* res = mysql_store_result(&scr_mysql);
  if (res == NULL) {
    scr_err("Result failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the number of rows in the result set */
  my_ulonglong nrows = mysql_num_rows(res);
  if (nrows != 1) {
    mysql_free_result(res);
    return SCR_FAILURE;
  }

  /* finally, lookup our id */
  MYSQL_ROW row = mysql_fetch_row(res);
  if (row == NULL) {
    scr_err("Row fetch failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    mysql_free_result(res);
    return SCR_FAILURE;
  }
  *id = strtoul(row[0], NULL, 0);

  /* free the result set */
  mysql_free_result(res);

#endif
  return SCR_SUCCESS;
}

/* lookup name in table, insert if it doesn't exist, and return id */
int scr_mysql_read_write_id(const char* table, const char* name, unsigned long* id)
{
  int rc = SCR_SUCCESS;

#ifdef HAVE_LIBMYSQLCLIENT
  /* if the value is already in the database, return its id */
  rc = scr_mysql_read_id(table, name, id);
  if (rc == SCR_SUCCESS) {
    return SCR_SUCCESS;
  }

  /* didn't find the value in the db, so let's add it */

  /* escape parameter */
  char* qname = scr_mysql_quote_string(name);

  /* check that we got valid strings for each of our parameters */
  if (qname == NULL) {
    scr_err("Failed to escape and quote one or more arguments @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* construct the query */
  char query[1024];
  int n = snprintf(query, sizeof(query),
    "INSERT IGNORE INTO `%s` (`id`,`name`) VALUES (NULL, %s) ;",
    table, qname
  );

  /* free the strings as they are now encoded into the query */
  scr_free(&qname);

  /* check that we were able to construct the query ok */
  if (n >= sizeof(query)) {
    scr_err("Insufficient buffer space (%lu bytes) to build query (%lu bytes) @ %s:%d",
            sizeof(query), n, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* execute the query */
  if (scr_db_debug >= 1) {
    scr_dbg(0, "%s", query);
  }
  if (mysql_real_query(&scr_mysql, query, (unsigned int) strlen(query))) {
    scr_err("Insert failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    /* don't return failure, since another process may have just beat us to the punch */
    /*return SCR_FAILURE;*/
  }

  /* alright, now we should be able to read the id */
  rc = scr_mysql_read_id(table, name, id);

#endif
  return rc;
}

/* lookups a type string and returns its id, 
 * inserts string into types table if not found,
 * caches lookups to avoid database reading more than once */
int scr_mysql_type_id(const char* type, int* id)
{
#ifdef HAVE_LIBMYSQLCLIENT
  unsigned long tmp_id;

  /* check that we don't have a NULL string */
  if (type == NULL) {
    scr_err("Type string is NULL @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* first check the hash in case we can avoid reading from the database */
  char* id_str = scr_hash_elem_get_first_val(scr_db_types, type);
  if (id_str != NULL) {
    /* found our id from the hash, convert to an unsigned long, then to an int */
    tmp_id = strtoul(id_str, NULL, 0);
    *id = (int) tmp_id;
    return SCR_SUCCESS;
  }

  /* lookup the id for our jobname */
  if (scr_mysql_read_write_id("types", type, &tmp_id) != SCR_SUCCESS) {
    scr_err("Failed to find type_id for %s @ %s:%d",
            type, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* got our id, now cache the lookup */
  scr_hash_setf(scr_db_types, NULL, "%s %lu", type, tmp_id);

  /* cast the id down to an int */
  *id = (int) tmp_id;
#endif
  return SCR_SUCCESS;
}

/* records an SCR event in the SCR log database */
int scr_mysql_log_event(const char* type, const char* note, const int* ckpt, const time_t* start, const double* secs)
{
#ifdef HAVE_LIBMYSQLCLIENT
  /* lookup the id for the type string */
  int type_id = -1;
  if (scr_mysql_type_id(type, &type_id) == SCR_FAILURE) {
    scr_err("Failed to lookup id for type string %s @ %s:%d",
            type, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  char* qnote  = scr_mysql_quote_string(note);
  char* qckpt  = scr_mysql_quote_int(ckpt);
  char* qstart = scr_mysql_quote_seconds(start);
  char* qsecs  = scr_mysql_quote_double(secs);

  /* check that we got valid strings for each of our parameters */
  if (qnote  == NULL ||
      qckpt  == NULL ||
      qstart == NULL ||
      qsecs  == NULL)
  {
    scr_err("Failed to escape and quote one or more arguments @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* construct the query */
  char query[4096];
  int n = snprintf(query, sizeof(query),
    "INSERT"
    " INTO `events`"
    " (`id`,`job_id`,`type_id`,`checkpoint_id`,`start`,`time`,`note`)"
    " VALUES"
    " (NULL, %lu, %d, %s, %s, %s, %s)"
    " ;",
    scr_db_jobid, type_id, qckpt, qstart, qsecs, qnote
  );

  /* free the strings as they are now encoded into the query */
  scr_free(&qnote);
  scr_free(&qckpt);
  scr_free(&qstart);
  scr_free(&qsecs);

  /* check that we were able to construct the query ok */
  if (n >= sizeof(query)) {
    scr_err("Insufficient buffer space (%lu bytes) to build query (%lu bytes) @ %s:%d",
            sizeof(query), n, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* execute the query */
  if (scr_db_debug >= 1) {
    scr_dbg(0, "%s", query);
  }
  if (mysql_real_query(&scr_mysql, query, (unsigned int) strlen(query))) {
    scr_err("Insert failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

#endif
  return SCR_SUCCESS;
}

/* records an SCR file transfer (checkpoint/fetch/flush/drain) in the SCR log database */
int scr_mysql_log_transfer(const char* type, const char* from, const char* to, const int* ckpt, const time_t* start, const double* secs, const double* bytes)
{
#ifdef HAVE_LIBMYSQLCLIENT
  /* lookup the id for the type string */
  int type_id = -1;
  if (scr_mysql_type_id(type, &type_id) == SCR_FAILURE) {
    scr_err("Failed to lookup id for type string %s @ %s:%d",
            type, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* compute end epoch, using trucation here */
  time_t* end = NULL;
  time_t end_val;
  if (start != NULL && secs != NULL) {
    end_val = *start + (time_t) *secs;
    end = &end_val;
  }

  /* compute the number of seconds and the bandwidth of the operation */
  double* bw = NULL;
  double bw_val;
  if (bytes != NULL && secs != NULL && *secs > 0.0) {
    bw_val = *bytes / *secs;
    bw = &bw_val;
  }
  
  /* convert seconds since epoch to mysql datetime strings */
  char* qfrom  = scr_mysql_quote_string(from);
  char* qto    = scr_mysql_quote_string(to);
  char* qckpt  = scr_mysql_quote_int(ckpt);
  char* qstart = scr_mysql_quote_seconds(start);
  char* qend   = scr_mysql_quote_seconds(end);
  char* qsecs  = scr_mysql_quote_double(secs);
  char* qbytes = scr_mysql_quote_double(bytes);
  char* qbw    = scr_mysql_quote_double(bw);

  /* check that we got valid strings for each of our parameters */
  if (qfrom  == NULL ||
      qto    == NULL ||
      qckpt  == NULL ||
      qstart == NULL ||
      qend   == NULL ||
      qsecs  == NULL ||
      qbytes == NULL ||
      qbw    == NULL)
  {
    scr_err("Failed to escape and quote one or more arguments @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* construct the query */
  char query[4096];
  int n = snprintf(query, sizeof(query),
    "INSERT"
    " INTO `transfers`"
    " (`id`,`job_id`,`type_id`,`checkpoint_id`,`start`,`end`,`time`,`bytes`,`bw`,`from`,`to`)"
    " VALUES"
    " (NULL, %lu, %d, %s, %s, %s, %s, %s, %s, %s, %s)"
    " ;",
    scr_db_jobid, type_id, qckpt, qstart, qend, qsecs, qbytes, qbw, qfrom, qto
  );

  /* free the strings as they are now encoded into the query */
  scr_free(&qfrom);
  scr_free(&qto);
  scr_free(&qckpt);
  scr_free(&qstart);
  scr_free(&qend);
  scr_free(&qsecs);
  scr_free(&qbytes);
  scr_free(&qbw);

  /* check that we were able to construct the query ok */
  if (n >= sizeof(query)) {
    scr_err("Insufficient buffer space (%lu bytes) to build query (%lu bytes) @ %s:%d",
            sizeof(query), n, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* execute the query */
  if (scr_db_debug >= 1) {
    scr_dbg(0, "%s", query);
  }
  if (mysql_real_query(&scr_mysql, query, (unsigned int) strlen(query))) {
    scr_err("Insert failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

#endif
  return SCR_SUCCESS;
}

int scr_mysql_read_job(unsigned long username_id, unsigned long jobname_id, unsigned long* id)
{
#ifdef HAVE_LIBMYSQLCLIENT
  /* TODO: need to escape parameters */
  /* construct the query */
  char query[1024];
  int n = snprintf(query, sizeof(query),
    "SELECT * FROM `jobs` WHERE `username_id` = '%lu' AND `jobname_id` = '%lu' ;",
    username_id, jobname_id
  );

  /* check that we were able to construct the query ok */
  if (n >= sizeof(query)) {
    scr_err("Insufficient buffer space (%lu bytes) to build query (%lu bytes) @ %s:%d",
            sizeof(query), n, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* execute the query */
  if (scr_db_debug >= 1) {
    scr_dbg(0, "%s", query);
  }
  if (mysql_real_query(&scr_mysql, query, (unsigned int) strlen(query))) {
    scr_err("Select failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* prepare the result set to be used */
  MYSQL_RES* res = mysql_store_result(&scr_mysql);
  if (res == NULL) {
    scr_err("Result failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the number of rows in the result set */
  my_ulonglong nrows = mysql_num_rows(res);
  if (nrows != 1) {
    mysql_free_result(res);
    return SCR_FAILURE;
  }

  /* finally, lookup our id */
  MYSQL_ROW row = mysql_fetch_row(res);
  if (row == NULL) {
    scr_err("Row fetch failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    mysql_free_result(res);
    return SCR_FAILURE;
  }
  *id = strtoul(row[0], NULL, 0);

  /* free the result set */
  mysql_free_result(res);

#endif
  return SCR_SUCCESS;
}

int scr_mysql_register_job(const char* username, const char* jobname, unsigned long start, unsigned long* jobid)
{
  int rc = SCR_SUCCESS;

#ifdef HAVE_LIBMYSQLCLIENT
  /* lookup the id for our username */
  unsigned long username_id;
  rc = scr_mysql_read_write_id("usernames", username, &username_id);
  if (rc != SCR_SUCCESS) {
    scr_err("Failed to find username_id for %s @ %s:%d",
            username, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* lookup the id for our jobname */
  unsigned long jobname_id;
  rc = scr_mysql_read_write_id("jobnames", jobname, &jobname_id);
  if (rc != SCR_SUCCESS) {
    scr_err("Failed to find jobname_id for %s @ %s:%d",
            jobname, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* if this job already has a db id, return it */
  rc = scr_mysql_read_job(username_id, jobname_id, jobid);
  if (rc == SCR_SUCCESS) {
    return SCR_SUCCESS;
  }

  /* didn't find the job, so we need to insert a new record into the db */

  /* translate unix seconds since epoch into mysql datetime field */
  time_t start_time_t = (time_t) start;
  char* qsecs = scr_mysql_quote_seconds(&start_time_t);

  /* check that we got valid strings for each of our parameters */
  if (qsecs == NULL) {
    scr_err("Failed to escape and quote one or more arguments @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* construct the query */
  char query[1024];
  int n = snprintf(query, sizeof(query),
    "INSERT IGNORE"
    " INTO `jobs`"
    " (`id`,`username_id`,`jobname_id`,`start`)"
    " VALUES"
    " (NULL, %lu, %lu, %s)"
    " ;",
    username_id, jobname_id, qsecs
  );

  /* free the strings as they are now encoded into the query */
  scr_free(&qsecs);

  /* check that we were able to construct the query ok */
  if (n >= sizeof(query)) {
    scr_err("Insufficient buffer space (%lu bytes) to build query (%lu bytes) @ %s:%d",
            sizeof(query), n, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* execute the query */
  if (scr_db_debug >= 1) {
    scr_dbg(0, "%s", query);
  }
  if (mysql_real_query(&scr_mysql, query, (unsigned int) strlen(query))) {
    scr_err("Insert failed, query = (%s), error = (%s) @ %s:%d",
            query, mysql_error(&scr_mysql), __FILE__, __LINE__
    );
    /* don't return failure, since another process may have just beat us to the punch */
    /*return SCR_FAILURE;*/
  }

  /* now the job should be in the db, so read again to get its id */
  rc = scr_mysql_read_job(username_id, jobname_id, jobid);

#endif
  return rc;
}

/*
=========================================
Log functions
=========================================
*/

/* returns the current linux timestamp */
time_t scr_log_seconds()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  time_t now = tv.tv_sec;
  return now;
}

/* initialize the logging */
int scr_log_init()
{
  int rc = SCR_SUCCESS;

  /* read in parameters */
  char* value = NULL;

  scr_param_init();

  /* check whether SCR logging DB is enabled */
  if ((value = scr_param_get("SCR_DB_ENABLE")) != NULL) {
    scr_db_enable = atoi(value);
  }

  /* read in the debug level for database log messages */
  if ((value = scr_param_get("SCR_DB_DEBUG")) != NULL) {
    scr_db_debug = atoi(value);
  }

  /* SCR log DB connection parameters */
  if ((value = scr_param_get("SCR_DB_HOST")) != NULL) {
    scr_db_host = strdup(value);
  }
  if ((value = scr_param_get("SCR_DB_USER")) != NULL) {
    scr_db_user = strdup(value);
  }
  if ((value = scr_param_get("SCR_DB_PASS")) != NULL) {
    scr_db_pass = strdup(value);
  }
  if ((value = scr_param_get("SCR_DB_NAME")) != NULL) {
    scr_db_name = strdup(value);
  }

  scr_param_finalize();

  /* connect to the database, if enabled */
  if (scr_db_enable) {
    if (scr_mysql_connect() != SCR_SUCCESS) {
      scr_err("Failed to connect to SCR logging database, disabling database logging @ %s:%d",
              __FILE__, __LINE__
      );
      scr_db_enable = 0;
      rc = SCR_FAILURE;
    }
  }

  return rc; 
}

/* shut down the logging */
int scr_log_finalize()
{
  /* disconnect from database */
  if (scr_db_enable) {
    scr_mysql_disconnect();
  }

  /* free memory */
  scr_free(&scr_db_host);
  scr_free(&scr_db_user);
  scr_free(&scr_db_pass);
  scr_free(&scr_db_name);

  return SCR_SUCCESS;
}

/* given a username, a jobname, and a start time, lookup (or create) the id for this job */
int scr_log_job(const char* username, const char* jobname, time_t start)
{
  int rc = SCR_SUCCESS;

  if (scr_db_enable) {
    if (username != NULL && jobname != NULL) {
      int rc = scr_mysql_register_job(username, jobname, start, &scr_db_jobid);
      if (rc != SCR_SUCCESS) {
        scr_err("Failed to register job for username %s and jobname %s, disabling database logging @ %s:%d",
                username, jobname, __FILE__, __LINE__
        );
        scr_db_enable = 0;
        rc = SCR_FAILURE;
      }
    } else {
      scr_err("Failed to read username or jobname from environment, disabling database logging @ %s:%d",
              __FILE__, __LINE__
      );
      scr_db_enable = 0;
      rc = SCR_FAILURE;
    }
  }

  return rc;
}

/* log start time of current run */
int scr_log_run(time_t start)
{
  int rc = SCR_SUCCESS;
  if (scr_db_enable) {
    rc = scr_mysql_log_event("START", NULL, NULL, &start, NULL);
  }
  return rc;
}

/* log reason and time for halting current run */
int scr_log_halt(const char* reason, const int* ckpt)
{
  int rc = SCR_SUCCESS;
  if (scr_db_enable) {
    time_t now = scr_log_seconds();
    rc = scr_mysql_log_event("HALT", reason, ckpt, &now, NULL);
  }
  return rc;
}

/* log an event */
int scr_log_event(const char* type, const char* note, const int* ckpt, const time_t* start, const double* secs)
{
  int rc = SCR_SUCCESS;
  if (scr_db_enable) {
    rc = scr_mysql_log_event(type, note, ckpt, start, secs);
  }
  return rc;
}

/* log a transfer: copy / checkpoint / fetch / flush */
int scr_log_transfer(const char* type, const char* from, const char* to, const int* ckpt_id, const time_t* start, const double* secs, const double* bytes)
{
  int rc = SCR_SUCCESS;
  if (scr_db_enable) {
    rc = scr_mysql_log_transfer(type, from, to, ckpt_id, start, secs, bytes);
  }
  return rc;
}
