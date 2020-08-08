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

/* Implements an interface to read/write SCR meta data files. */

#include "scr_globals.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_io.h"
#include "scr_meta.h"

#include "spath.h"
#include "kvtree.h"

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

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

/* compute crc32 */
#include <zlib.h>


/* TODO: ugly hack until we get a configure test */
#if defined(__APPLE__)
#define HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC 1
#else
#define HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC 1
#endif
// HAVE_STRUCT_STAT_ST_MTIME_N
// HAVE_STRUCT_STAT_ST_UMTIME
// HAVE_STRUCT_STAT_ST_MTIME_USEC

/*
=========================================
Allocate, delete, and copy functions
=========================================
*/

/* allocate a new meta data object */
scr_meta* scr_meta_new()
{
  scr_meta* meta = kvtree_new();
  if (meta == NULL) {
    scr_err("Failed to allocate meta data object @ %s:%d", __FILE__, __LINE__);
  }
  return meta;
}

/* free memory assigned to meta data object */
int scr_meta_delete(scr_meta** ptr_meta)
{
  int rc = kvtree_delete(ptr_meta);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* clear m1 and copy contents of m2 into m1 */
int scr_meta_copy(scr_meta* m1, const scr_meta* m2)
{
  kvtree_unset_all(m1);
  int rc = kvtree_merge(m1, m2);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/*
=========================================
Set field values
=========================================
*/

/* sets the checkpoint id in meta data to be the value specified */
int scr_meta_set_checkpoint(scr_meta* meta, int ckpt)
{
  kvtree_unset(meta, SCR_META_KEY_CKPT);
  kvtree_set_kv_int(meta, SCR_META_KEY_CKPT, ckpt);
  return SCR_SUCCESS;
}

/* sets the rank in meta data to be the value specified */
int scr_meta_set_rank(scr_meta* meta, int rank)
{
  kvtree_unset(meta, SCR_META_KEY_RANK);
  kvtree_set_kv_int(meta, SCR_META_KEY_RANK, rank);
  return SCR_SUCCESS;
}

/* sets the rank in meta data to be the value specified */
int scr_meta_set_ranks(scr_meta* meta, int ranks)
{
  kvtree_unset(meta, SCR_META_KEY_RANKS);
  kvtree_set_kv_int(meta, SCR_META_KEY_RANKS, ranks);
  return SCR_SUCCESS;
}

/* sets the original filename value in meta data */
int scr_meta_set_orig(scr_meta* meta, const char* file)
{
  kvtree_unset(meta, SCR_META_KEY_ORIG);
  kvtree_set_kv(meta, SCR_META_KEY_ORIG, file);
  return SCR_SUCCESS;
}

/* sets the full path to the original filename value in meta data */
int scr_meta_set_origpath(scr_meta* meta, const char* file)
{
  kvtree_unset(meta, SCR_META_KEY_PATH);
  kvtree_set_kv(meta, SCR_META_KEY_PATH, file);
  return SCR_SUCCESS;
}

/* sets the full directory to the original filename value in meta data */
int scr_meta_set_origname(scr_meta* meta, const char* file)
{
  kvtree_unset(meta, SCR_META_KEY_NAME);
  kvtree_set_kv(meta, SCR_META_KEY_NAME, file);
  return SCR_SUCCESS;
}

/* sets the filesize to be the value specified */
int scr_meta_set_filesize(scr_meta* meta, unsigned long filesize)
{
  int rc = kvtree_util_set_bytecount(meta, SCR_META_KEY_SIZE, filesize);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* sets complete value in meta data, overwrites any existing value with new value */
int scr_meta_set_complete(scr_meta* meta, int complete)
{
  kvtree_unset(meta, SCR_META_KEY_COMPLETE);
  kvtree_set_kv_int(meta, SCR_META_KEY_COMPLETE, complete);
  return SCR_SUCCESS;
}

/* sets crc value in meta data, overwrites any existing value with new value */
int scr_meta_set_crc32(scr_meta* meta, uLong crc)
{
  int rc = kvtree_util_set_crc32(meta, SCR_META_KEY_CRC, crc);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

static void scr_stat_get_atimes(const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_atime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_atimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_atim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_atime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_uatime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_atime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

static void scr_stat_get_mtimes (const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_mtime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_mtimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_mtim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_mtime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_umtime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_mtime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

static void scr_stat_get_ctimes (const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_ctime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_ctimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_ctim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_ctime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_uctime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_ctime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

int scr_meta_set_stat(scr_meta* meta, struct stat* sb)
{
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_MODE, (unsigned long) sb->st_mode);
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_UID,  (unsigned long) sb->st_uid);
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_GID,  (unsigned long) sb->st_gid);
  //kvtree_util_set_unsigned_long(meta, SCR_META_KEY_SIZE, (unsigned long) sb->st_size);

  uint64_t secs, nsecs;
  scr_stat_get_atimes(sb, &secs, &nsecs);
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_ATIME_SECS,  (unsigned long) secs);
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_ATIME_NSECS, (unsigned long) nsecs);

  scr_stat_get_ctimes(sb, &secs, &nsecs);
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_CTIME_SECS,  (unsigned long) secs);
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_CTIME_NSECS, (unsigned long) nsecs);

  scr_stat_get_mtimes(sb, &secs, &nsecs);
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_MTIME_SECS,  (unsigned long) secs);
  kvtree_util_set_unsigned_long(meta, SCR_META_KEY_MTIME_NSECS, (unsigned long) nsecs);

  return SCR_SUCCESS;
}

/*
=========================================
Get field values
=========================================
*/

/* gets checkpoint id recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_checkpoint(const scr_meta* meta, int* ckpt)
{
  int rc = kvtree_util_get_int(meta, SCR_META_KEY_CKPT, ckpt);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets rank value recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_rank(const scr_meta* meta, int* rank)
{
  int rc = kvtree_util_get_int(meta, SCR_META_KEY_RANK, rank);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets ranks value recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_ranks(const scr_meta* meta, int* ranks)
{
  int rc = kvtree_util_get_int(meta, SCR_META_KEY_RANKS, ranks);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_orig(const scr_meta* meta, char** filename)
{
  int rc = kvtree_util_get_str(meta, SCR_META_KEY_ORIG, filename);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets full path to the original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_origpath(const scr_meta* meta, char** filename)
{
  int rc = kvtree_util_get_str(meta, SCR_META_KEY_PATH, filename);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets the name of the original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_origname(const scr_meta* meta, char** filename)
{
  int rc = kvtree_util_get_str(meta, SCR_META_KEY_NAME, filename);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets filesize recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filesize(const scr_meta* meta, unsigned long* filesize)
{
  int rc = kvtree_util_get_bytecount(meta, SCR_META_KEY_SIZE, filesize);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* get the completeness field in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_complete(const scr_meta* meta, int* complete)
{
  int rc = kvtree_util_get_int(meta, SCR_META_KEY_COMPLETE, complete);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* get the crc32 field in meta data, returns SCR_SUCCESS if a field is set */
int scr_meta_get_crc32(const scr_meta* meta, uLong* crc)
{
  int rc = kvtree_util_get_crc32(meta, SCR_META_KEY_CRC, crc);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/*
=========================================
Check field values
=========================================
*/

/* return SCR_SUCCESS if meta data is marked as complete */
int scr_meta_is_complete(const scr_meta* meta)
{
  int complete = 0;
  if (kvtree_util_get_int(meta, SCR_META_KEY_COMPLETE, &complete) == KVTREE_SUCCESS) {
    if (complete == 1) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if rank is set in meta data, and if it matches the specified value */
int scr_meta_check_rank(const scr_meta* meta, int rank)
{
  int rank_meta;
  if (kvtree_util_get_int(meta, SCR_META_KEY_RANK, &rank_meta) == KVTREE_SUCCESS) {
    if (rank == rank_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if ranks is set in meta data, and if it matches the specified value */
int scr_meta_check_ranks(const scr_meta* meta, int ranks)
{
  int ranks_meta;
  if (kvtree_util_get_int(meta, SCR_META_KEY_RANKS, &ranks_meta) == KVTREE_SUCCESS) {
    if (ranks == ranks_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if checkpoint_id is set in meta data, and if it matches the specified value */
int scr_meta_check_checkpoint(const scr_meta* meta, int ckpt)
{
  int ckpt_meta;
  if (kvtree_util_get_int(meta, SCR_META_KEY_CKPT, &ckpt_meta) == KVTREE_SUCCESS) {
    if (ckpt == ckpt_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* returns SCR_SUCCESS if filesize is set in meta data, and if it matches specified value */
int scr_meta_check_filesize(const scr_meta* meta, unsigned long filesize)
{
  unsigned long filesize_meta = 0;
  if (kvtree_util_get_bytecount(meta, SCR_META_KEY_SIZE, &filesize_meta) == KVTREE_SUCCESS) {
    if (filesize == filesize_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

int scr_meta_check_mtime(const scr_meta* meta, struct stat* sb)
{
  unsigned long secs_meta, nsecs_meta;
  if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_MTIME_SECS,  &secs_meta) == KVTREE_SUCCESS) {
    if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_MTIME_NSECS, &nsecs_meta) == KVTREE_SUCCESS) {
      uint64_t secs, nsecs;
      scr_stat_get_mtimes(sb, &secs, &nsecs);
      if (secs == secs_meta && nsecs == nsecs_meta) {
        return SCR_SUCCESS;
      }
    }
  }
  return SCR_FAILURE;
}

int scr_meta_check_ctime(const scr_meta* meta, struct stat* sb)
{
  unsigned long secs_meta, nsecs_meta;
  if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_CTIME_SECS,  &secs_meta) == KVTREE_SUCCESS) {
    if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_CTIME_NSECS, &nsecs_meta) == KVTREE_SUCCESS) {
      uint64_t secs, nsecs;
      scr_stat_get_ctimes(sb, &secs, &nsecs);
      if (secs == (uint64_t)secs_meta && nsecs == (uint64_t)nsecs_meta) {
        return SCR_SUCCESS;
      }
    }
  }
  return SCR_FAILURE;
}

int scr_meta_check_metadata(const scr_meta* meta, struct stat* sb)
{
  /* check that the mode bits (permissions) have not changed */
  unsigned long mode;
  if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_MODE, &mode) != KVTREE_SUCCESS) {
    return SCR_FAILURE;
  }
  if (mode != (unsigned long)sb->st_mode) {
    return SCR_FAILURE;
  }

  /* check that the user id for the owner has not changed */
  unsigned long uid;
  if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_UID, &uid) != KVTREE_SUCCESS) {
    return SCR_FAILURE;
  }
  if (uid != (unsigned long)sb->st_uid) {
    return SCR_FAILURE;
  }

  /* check that the group id has not changed */
  unsigned long gid;
  if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_GID, &gid) != KVTREE_SUCCESS) {
    return SCR_FAILURE;
  }
  if (gid != (unsigned long)sb->st_gid) {
    return SCR_FAILURE;
  }

  /* everything checks out */
  return SCR_SUCCESS;
}

int scr_meta_apply_stat(const scr_meta* meta, const char* file)
{
  int rc = SCR_SUCCESS;

  /* set permission bits on file */
  unsigned long mode_val;
  if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_MODE, &mode_val) == KVTREE_SUCCESS) {
    mode_t mode = (mode_t) mode_val;

    /* TODO: mask some bits here */

    int chmod_rc = chmod(file, mode);
    if (chmod_rc != 0) {
      /* failed to set permissions */
      scr_err("chmod(%s) failed: errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  /* set uid and gid on file */
  unsigned long uid_val = -1;
  unsigned long gid_val = -1;
  kvtree_util_get_unsigned_long(meta, SCR_META_KEY_UID, &uid_val);
  kvtree_util_get_unsigned_long(meta, SCR_META_KEY_GID, &gid_val);
  if (uid_val != -1 || gid_val != -1) {
    /* got a uid or gid value, try to set them */
    int chown_rc = chown(file, (uid_t) uid_val, (gid_t) gid_val);
    if (chown_rc != 0) {
      /* failed to set uid and gid */
      scr_err("chown(%s, %lu, %lu) failed: errno=%d %s @ %s:%d",
        file, uid_val, gid_val, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  /* can't set the size at this point, but we can check it */
  unsigned long size;
  if (kvtree_util_get_unsigned_long(meta, SCR_META_KEY_SIZE, &size) == KVTREE_SUCCESS) {
    /* got a size field in the metadata, stat the file */
    struct stat statbuf;
    int stat_rc = lstat(file, &statbuf);
    if (stat_rc == 0) {
      /* stat succeeded, check that sizes match */
      if (size != statbuf.st_size) {
        /* file size is not correct */
        scr_err("file `%s' size is %lu expected %lu @ %s:%d",
          file, (unsigned long) statbuf.st_size, size, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }
    } else {
      /* failed to stat file */
      scr_err("stat(%s) failed: errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  /* set timestamps on file as last step */
  unsigned long atime_secs  = 0;
  unsigned long atime_nsecs = 0;
  kvtree_util_get_unsigned_long(meta, SCR_META_KEY_ATIME_SECS,  &atime_secs);
  kvtree_util_get_unsigned_long(meta, SCR_META_KEY_ATIME_NSECS, &atime_nsecs);

  unsigned long mtime_secs  = 0;
  unsigned long mtime_nsecs = 0;
  kvtree_util_get_unsigned_long(meta, SCR_META_KEY_MTIME_SECS,  &mtime_secs);
  kvtree_util_get_unsigned_long(meta, SCR_META_KEY_MTIME_NSECS, &mtime_nsecs);

  if (atime_secs != 0 || atime_nsecs != 0 ||
      mtime_secs != 0 || mtime_nsecs != 0)
  {
    /* fill in time structures */
    struct timespec times[2];
    times[0].tv_sec  = (time_t) atime_secs;
    times[0].tv_nsec = (long)   atime_nsecs;
    times[1].tv_sec  = (time_t) mtime_secs;
    times[1].tv_nsec = (long)   mtime_nsecs;

    /* set times with nanosecond precision using utimensat,
     * assume path is relative to current working directory,
     * if it's not absolute, and set times on link (not target file)
     * if dest_path refers to a link */
    int utime_rc = utimensat(AT_FDCWD, file, times, AT_SYMLINK_NOFOLLOW);
    if (utime_rc != 0) {
      scr_err("Failed to change timestamps on `%s' utimensat() errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  return rc;
}
