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

/* Reads configuration file and return values in hash of following form:
 *
 * # Comments set off with '#' and blank lines are ok
 * # Note that '#' and '=' are reserved characters;
 * # they cannot be used in key or value strings.
 *
 * # Each entry either contains a single key/value pair separated by '=' sign
 *
 * SCR_VARIABLE=VALUE # Each line can end with a trailing comment
 *
 * # Or an entry contains a parent key/value pair with a series of
 * # one or more child key/value pairs
 *
 * PARENT_KEY=PARENT_VALUE CHILD_KEY1=CHILD_VALUE1 CHILD_KEY2=CHILD_VALUE2 ...
 * */

#include "scr_conf.h"
#include "scr.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_io.h"
#include "scr_hash.h"
#include "scr_config.h"

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

/* toupper */
#include <ctype.h>

/* read in whitespace (spaces and tabs) until we hit a non-whitespace character */
static int scr_config_read_whitespace(
  FILE* fs, const char* file, int linenum,
  int* n_external, char* c_external)
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

/* read token into buffer of specified max size,
 * stops at whitespace, newline, or '=' character */
static int scr_config_read_token(
  FILE* fs, const char* file, int linenum,
  int* n_external, char* c_external, char* token, int size)
{
  /* remove whitespace (spaces and tabs) until we hit a character */
  int  n = *n_external;
  char c = *c_external;

  /* read bytes of token into buffer */
  int i = 0;
  while (n != EOF && c != ' ' && c != '\t' && c != '\n' && c != '=') {
    /* check that we don't overwrite our token buffer */
    if (i >= size) {
      scr_err("Internal buffer too short (%d bytes) while reading token in configuration file @ %s:%d",
        size, file, linenum
      );
      return SCR_FAILURE;
    }

    /* copy next character into token buffer */
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

/* read in a key value pair and insert into hash,
 * return hash under value in hash2 pointer,
 * (key)(\s*)(=)(\s*)(value) */
static int scr_config_read_kv(
  FILE* fs, const char* file, int linenum,
  int* n_external, char* c_external, scr_hash* hash, scr_hash** hash2)
{
  int  n = *n_external;
  char c = *c_external;

  char key[SCR_MAX_FILENAME];
  char value[SCR_MAX_FILENAME];

  /* read in the key token */
  scr_config_read_token(fs, file, linenum, &n, &c, key, sizeof(key));

  /* optional white space between key and '=' sign */
  scr_config_read_whitespace(fs, file, linenum, &n, &c);

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
  scr_config_read_whitespace(fs, file, linenum, &n, &c);

  /* read in the value token */
  scr_config_read_token(fs, file, linenum, &n, &c, value, sizeof(value));

  /* convert key to upper case */
  int i = 0;
  for (i=0; i < strlen(key); i++) {
    key[i] = (char) toupper(key[i]);
  }

  /* insert key/value into hash */
  scr_hash* tmp_hash = scr_hash_set_kv(hash, key, value);

  *n_external = n;
  *c_external = c;
  *hash2 = tmp_hash;

  return SCR_SUCCESS;
}

/* found a comment, strip out everything until we hit a newline or end-of-file */
static int scr_config_read_comment(
  FILE* fs, const char* file, int linenum,
  int* n_external, char* c_external)
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

/* process all items found on the current line
 * and record any entries in hash */
static int scr_config_read_line(
  FILE* fs, const char* file, int linenum,
  int* n_external, char* c_external, scr_hash* hash)
{
  int  n = *n_external;
  char c = *c_external;

  /* go until we hit a newline of end-of-file */
  scr_hash* target_hash = hash;
  int set_root = 0;
  while (n != EOF && c != '\n') {
    /* remove whitespace (spaces and tabs) until we hit a character */
    scr_config_read_whitespace(fs, file, linenum, &n, &c);

    /* if the line didn't end, we must have found a comment or another key/value pair */
    if (n != EOF && c != '\n') {
      if (c == '#') {
        /* found the start of a comment, strip it out */
        scr_config_read_comment(fs, file, linenum, &n, &c);
      } else {
        /* otherwise, must have a key value chain, so read them in */
        scr_hash* tmp_hash;
        scr_config_read_kv(fs, file, linenum, &n, &c, target_hash, &tmp_hash);

        /* we set the first key/value pair as the root,
         * if there are other key/value pairs on the same line,
         * we store all of those as children under the parent */
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

/* read parameters from config file and fill in hash */
int scr_config_read_common(const char* file, scr_hash* hash)
{
  /* check whether we can read the file */
  if (scr_file_is_readable(file) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* open the file */
  FILE* fs = fopen(file, "r");
  if (fs == NULL) {
    scr_err("Opening configuration file for read: fopen(%s, \"r\") errno=%d %s @ %s:%d",
            file, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read the file until we hit an end-of-file */
  int n;
  char c;
  int linenum = 0;
  do {
    /* keep track of our line number for more useful error messages */
    linenum++;

    /* read in the first character of the line to prime our n and c values */
    n = fgetc(fs);
    c = (char) n;

    /* now process the line */
    scr_config_read_line(fs, file, linenum, &n, &c, hash);
  } while (n != EOF);

  /* close the file */
  fclose(fs);

  return SCR_SUCCESS;
}
