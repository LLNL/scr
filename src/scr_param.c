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

/* Reads parameters from environment and configuration files */

#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_hash.h"
#include "scr_param.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef SCR_CONFIG_FILE
#define SCR_CONFIG_FILE "/etc/scr.conf"
#endif

/* since multiple objects may require parameters, and thus init and finalize,
 * we keep a reference so we don't clear the data structures until all modules
 * which currently are using the object have finished */
static int scr_param_ref_count = 0;

/* name of system configuration file we should read */
static char scr_config_file[SCR_MAX_FILENAME] = SCR_CONFIG_FILE;

/* this data structure will hold values read from the system config file */
static struct scr_hash* scr_config_hash = NULL;

/* TODO: support processing of byte values */
/*

#define KILO (1024)
#define MEGA (1024*KILO)
#define GIGA (1024*MEGA)
#define TERA (1024*GIGA)

// converts string like 10mb to long long integer value of 10*1024*1024
size_t scr_param_abtoll(char* str)
{
  char* next;
  size_t units = 1;

  double num = strtod(str, &next);
  if (num == 0.0 && next == str) {
    return 0;
  }

  if (*next != '\0') {
    // process units for kilo, mega, or gigabytes
    switch(*next) {
    case 'k':
    case 'K':
      units = (size_t) KILO;
      break;
    case 'm':
    case 'M':
      units = (size_t) MEGA;
      break;
    case 'g':
    case 'G':
      units = (size_t) GIGA;
      break;
    case 't':
    case 'T':
      units = (size_t) TERA;
      break;
    default:
      printf("ERROR:  unexpected byte string %s\n", str);
      exit(1);
    }

    next++;

    // handle optional b or B character, e.g. in 10KB
    if (*next == 'b' || *next == 'B') {
      next++;
    }

    if (*next != 0) {
      printf("ERROR:  unexpected byte string: %s\n", str);
      exit(1);
    }
  }

  if (num < 0) {
    printf("ERROR:  byte string must be positive: %s\n", str);
    exit(1);
  }

  size_t val = (size_t) (num * (double) units);
  return val;
}
*/


/* read in whitespace until we hit a non-whitespace character */
static int scr_param_read_config_whitespace(FILE* fs, char* file, int linenum, int* n_external, char* c_external)
{
  /* remove whitespace (spaces and tabs) until we hit a character */
  int  n = *n_external;
  char c = *c_external;
  while (n != EOF && (c == ' ' || c == '\t')) {
    n = fgetc(fs);
    c = (char) n;
  }
  *n_external = n;
  *c_external = c;
  return SCR_SUCCESS;
}

/* read in a token */
static int scr_param_read_config_token(FILE* fs, char* file, int linenum, int* n_external, char* c_external, char* token, int size)
{
  /* remove whitespace (spaces and tabs) until we hit a character */
  int  n = *n_external;
  char c = *c_external;

  /* read bytes of token into buffer */
  int i = 0;
  while (n != EOF && c != ' ' && c != '\t' && c != '\n' && c != '=') {
    if (i >= size) {
      scr_err("Internal buffer too short (%d bytes) while reading token in configuration file @ %s:%d",
              size, file, linenum
      );
      return SCR_FAILURE;
    }
    token[i++] = c;

    n = fgetc(fs);
    c = (char) n;
  }

  /* check that our token is at least one character long */
  if (i == 0) {
    scr_err("Missing token in configuration file @ %s:%d",
            file, linenum
    );
    return SCR_FAILURE;
  }

  /* terminate our token with a null character */
  if (i >= size) {
    scr_err("Internal buffer too short (%d bytes) while reading token in configuration file @ %s:%d",
            size, file, linenum
    );
    return SCR_FAILURE;
  }
  token[i++] = '\0';

  *n_external = n;
  *c_external = c;
  return SCR_SUCCESS;
}

/* found a key value pair, read in the values and insert it into the hash */
static int scr_param_read_config_kv(FILE* fs, char* file, int linenum,
                                    int* n_external, char* c_external, struct scr_hash* hash, struct scr_hash** hash2)
{
  int  n = *n_external;
  char c = *c_external;

  char key[SCR_MAX_FILENAME];
  char value[SCR_MAX_FILENAME];

  /* read in the key token */
  scr_param_read_config_token(fs, file, linenum, &n, &c, key, sizeof(key));

  /* optional white space between key and '=' sign */
  scr_param_read_config_whitespace(fs, file, linenum, &n, &c);

  /* should be sitting on the '=' that splits the key and value at this point */
  if (c != '=') {
    scr_err("Ill-formed key value pair detected in configuration file @ %s:%d",
            file, linenum
    );
    return SCR_FAILURE;
  }
  n = fgetc(fs);
  c = (char) n;

  /* optional white space between '=' sign and value */
  scr_param_read_config_whitespace(fs, file, linenum, &n, &c);

  /* read in the value token */
  scr_param_read_config_token(fs, file, linenum, &n, &c, value, sizeof(value));

  /* convert key to upper case */
  int i = 0;
  for (i=0; i < strlen(key); i++) {
    key[i] = (char) toupper(key[i]);
  }

  /* insert key/value into hash */
  struct scr_hash* tmp_hash = scr_hash_set_kv(hash, key, value);

  *n_external = n;
  *c_external = c;
  *hash2 = tmp_hash;

  return SCR_SUCCESS;
}

/* found a comment, strip it out */
static int scr_param_read_config_comment(FILE* fs, char* file, int linenum, int* n_external, char* c_external)
{
  /* inside a comment, remove the rest of the line */
  int  n = *n_external;
  char c = *c_external;
  while (n != EOF && c != '\n') {
    n = fgetc(fs);
    c = (char) n;
  }
  *n_external = n;
  *c_external = c;
  return SCR_SUCCESS;
}

/* process all items found on the current line from the config file */
static int scr_param_read_config_line(FILE* fs, char* file, int linenum, int* n_external, char* c_external, struct scr_hash* hash)
{
  int  n = *n_external;
  char c = *c_external;

  struct scr_hash* target_hash = hash;
  int set_root = 0;
  while (n != EOF && c != '\n') {
    /* remove whitespace (spaces and tabs) until we hit a character */
    scr_param_read_config_whitespace(fs, file, linenum, &n, &c);

    if (n != EOF && c != '\n') {
      if (c == '#') {
        scr_param_read_config_comment(fs, file, linenum, &n, &c);
      } else {
        /* must have a key value chain, so read them in */
        struct scr_hash* tmp_hash = NULL;
        scr_param_read_config_kv(fs, file, linenum, &n, &c, target_hash, &tmp_hash);
        if (set_root == 0) {
          target_hash = tmp_hash;
          set_root = 1;
        }
      }
    }
  }

  *n_external = n;
  *c_external = c;
  return SCR_SUCCESS;
}

/* read parameters from system config file and fill in hash */
static int scr_param_read_config(char* file, struct scr_hash* hash)
{
  /* check whether we can read the config file */
  if (access(file, R_OK) < 0) {
    return SCR_FAILURE;
  }

  /* open the config file */
  FILE* fs = fopen(file, "r");
  if (fs == NULL) {
    scr_err("Opening configuration file for read: fopen(%s, \"r\") errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read the config file */
  int n;
  char c;
  int linenum = 0;
  do {
    linenum++;
    /* read in the first character of the line */
    n = fgetc(fs);
    c = (char) n;
    scr_param_read_config_line(fs, file, linenum, &n, &c, hash);
  } while (n != EOF);

  /* close the file */
  fclose(fs);

  return SCR_SUCCESS;
}

/* given a parameter name like SCR_FLUSH, return its value checking the following order:
 *   environment variable
 *   system config file
*/
char* scr_param_get(char* name)
{
  /* TODO: certain values may be environment only, others may be config file only */

  /* if parameter is set in environment, return that value */
  if (getenv(name) != NULL) {
    /* TODO: need to strdup here to be safe? */
    return getenv(name);
  }

  /* otherwise, if parameter is set in configuration file, return that value */
  struct scr_hash* key_hash = scr_hash_get(scr_config_hash, name);
  if (key_hash != NULL) {
    /* TODO: need to handle hash values with multiple entries here? */
    /* get the value hash */
    struct scr_hash_elem* value_elem = scr_hash_elem_first(key_hash);
    if (value_elem != NULL) {
      /* finally, return the value string */
      return scr_hash_elem_key(value_elem);
    }
  }

  return NULL;
}

int scr_param_init()
{
  if (scr_param_ref_count == 0) {
    /* allocate hash objects to hold values from configuration files */
    scr_config_hash = scr_hash_new();

    /* read parameters from configuration file */
    scr_param_read_config(scr_config_file, scr_config_hash);

  }

  /* increment our reference count */
  scr_param_ref_count++;

  return SCR_SUCCESS;
}

int scr_param_finalize()
{
  /* decrement our reference count */
  scr_param_ref_count--;

  /* if the reference count is zero, free the data structures */
  if (scr_param_ref_count == 0) {
    /* free our parameter hash */
    scr_hash_delete(scr_config_hash);
  }

  return SCR_SUCCESS;
}
