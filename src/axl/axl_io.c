/* CRC and uLong */
#include <zlib.h>

/* mode bits */
#include <sys/stat.h>

/* file open modes */
#include <sys/file.h>
#include <fcntl.h>

/* exit & malloc */
#include <stdlib.h>

// dirname and basename
#include <libgen.h>

#include <errno.h>
#include <string.h>

#include <unistd.h>
#include <sys/types.h>
#include <stdint.h>
#include <limits.h>

#include <stdio.h>

#include "axl_internal.h"

/* Configurations */
#ifndef AXL_OPEN_TRIES
#define AXL_OPEN_TRIES (5)
#endif

#ifndef AXL_OPEN_USLEEP
#define AXL_OPEN_USLEEP (100)
#endif

/* TODO: ugly hack until we get a configure test */
#if defined(__APPLE__)
#define HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC 1
#else
#define HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC 1
#endif
// HAVE_STRUCT_STAT_ST_MTIME_N
// HAVE_STRUCT_STAT_ST_UMTIME
// HAVE_STRUCT_STAT_ST_MTIME_USEC

/* returns user's current mode as determine by their umask */
mode_t axl_getmode(int read, int write, int execute)
{
    /* lookup current mask and set it back */
    mode_t old_mask = umask(S_IWGRP | S_IWOTH);
    umask(old_mask);

    mode_t bits = 0;
    if (read) {
        bits |= (S_IRUSR | S_IRGRP | S_IROTH);
    }
    if (write) {
        bits |= (S_IWUSR | S_IWGRP | S_IWOTH);
    }
    if (execute) {
        bits |= (S_IXUSR | S_IXGRP | S_IXOTH);
    }

    /* convert mask to mode */
    mode_t mode = bits & ~old_mask & 0777;
    return mode;
}

/* recursively create directory and subdirectories */
int axl_mkdir(const char* dir, mode_t mode)
{
    /* consider it a success if we either create the directory
     * or we fail because it already exists */
    int tmp_rc = mkdir(dir, mode);
    if (tmp_rc == 0 || errno == EEXIST) {
        return AXL_SUCCESS;
    }

    /* failed to create the directory,
     * we'll check the parent dir and try again */
    int rc = AXL_SUCCESS;

    /* With dirname, either the original string may be modified or the function may return a
     * pointer to static storage which will be overwritten by the next call to dirname,
     * so we need to strdup both the argument and the return string. */

    /* extract leading path from dir = full path - basename */
    char* dircopy = strdup(dir);
    char* path    = strdup(dirname(dircopy));

    /* if we can read path or path=="." or path=="/", then there's nothing to do,
     * otherwise, try to create it */
    if (access(path, R_OK) < 0 &&
        strcmp(path,".") != 0  &&
        strcmp(path,"/") != 0)
    {
        rc = axl_mkdir(path, mode);
    }

    /* if we can write to path, try to create subdir within path */
    if (access(path, W_OK) == 0 && rc == AXL_SUCCESS) {
        tmp_rc = mkdir(dir, mode);
        if (tmp_rc < 0) {
            if (errno == EEXIST) {
                /* don't complain about mkdir for a directory that already exists */
                axl_free(&dircopy);
                axl_free(&path);
                return AXL_SUCCESS;
            } else {
                AXL_ERR("Creating directory: mkdir(%s, %x) path=%s errno=%d %s",
                    dir, mode, path, errno, strerror(errno)
                );
                rc = AXL_FAILURE;
            }
        }
    } else {
        AXL_ERR("Cannot write to directory: %s", path);
        rc = AXL_FAILURE;
    }

    /* free our dup'ed string and return error code */
    axl_free(&dircopy);
    axl_free(&path);
    return rc;
}

/* delete a file */
int axl_file_unlink(const char* file)
{
    if (unlink(file) != 0) {
        AXL_DBG(2, "Failed to delete file: %s errno=%d %s",
            file, errno, strerror(errno)
        );
        return AXL_FAILURE;
    }
    return AXL_SUCCESS;
}

/* open file with specified flags and mode, retry open a few times on failure */
int axl_open(const char* file, int flags, ...)
{
    /* extract the mode (see man 2 open) */
    int mode_set = 0;
    mode_t mode = 0;
    if (flags & O_CREAT) {
        va_list ap;
        va_start(ap, flags);
        mode = va_arg(ap, int);
        va_end(ap);
        mode_set = 1;
    }

    int fd = -1;
    if (mode_set) {
        fd = open(file, flags, mode);
    } else {
        fd = open(file, flags);
    }

    if (fd < 0) {
        AXL_DBG(1, "Opening file: open(%s) errno=%d %s",
            file, errno, strerror(errno)
        );

        /* try again */
        int tries = AXL_OPEN_TRIES;
        while (tries && fd < 0) {
            usleep(AXL_OPEN_USLEEP);
            if (mode_set) {
                fd = open(file, flags, mode);
            } else {
                fd = open(file, flags);
            }
            tries--;
        }

        /* if we still don't have a valid file, consider it an error */
        if (fd < 0) {
            AXL_ERR("Opening file: open(%s) errno=%d %s",
                    file, errno, strerror(errno)
                    );
        }
    }

    return fd;
}

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t axl_read(const char* file, int fd, void* buf, unsigned long size)
{
    ssize_t n = 0;
    int retries = 10;
    while (n < size) {
        int rc = read(fd, (char*) buf + n, size - n);
        if (rc  > 0) {
            n += rc;
        } else if (rc == 0) {
            /* EOF */
            return n;
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }

            /* something worth printing an error about */
            retries--;
            if (retries) {
                /* print an error and try again */
                AXL_ERR("Error reading %s: read(%d, %x, %ld) errno=%d %s",
                    file, fd, (char*) buf + n, size - n, errno, strerror(errno)
                );
            } else {
                /* too many failed retries, give up */
                AXL_ERR("Giving up read of %s: read(%d, %x, %ld) errno=%d %s",
                    file, fd, (char*) buf + n, size - n, errno, strerror(errno)
                );
                exit(1);
            }
        }
    }
    return n;
}

/* make a good attempt to read from file (retries, if necessary, return error if fail) */
ssize_t axl_read_attempt(const char* file, int fd, void* buf, unsigned long size)
{
    ssize_t n = 0;
    int retries = 10;
    while (n < size) {
        int rc = read(fd, (char*) buf + n, size - n);
        if (rc  > 0) {
            n += rc;
        } else if (rc == 0) {
            /* EOF */
            return n;
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }

            /* something worth printing an error about */
            retries--;
            if (retries) {
                /* print an error and try again */
                AXL_ERR("Error reading file %s errno=%d %s",
                    file, errno, strerror(errno)
                );
            } else {
                /* too many failed retries, give up */
                AXL_ERR("Giving up read on file %s errno=%d %s",
                    file, errno, strerror(errno)
                );
                return -1;
            }
        }
    }
    return n;
}


/* make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t axl_write_attempt(const char* file, int fd, const void* buf, unsigned long size)
{
    ssize_t n = 0;
    int retries = 10;
    while (n < size) {
        ssize_t rc = write(fd, (char*) buf + n, size - n);
        if (rc > 0) {
            n += rc;
        } else if (rc == 0) {
            /* something bad happened, print an error and abort */
            AXL_ERR("Error writing file %s write returned 0", file);
            return -1;
        } else { /* (rc < 0) */
            /* got an error, check whether it was serious */
            if (errno == EINTR || errno == EAGAIN) {
                continue;
            }

            /* something worth printing an error about */
            retries--;
            if (retries) {
                /* print an error and try again */
                AXL_ERR("Error writing file %s errno=%d %s",
                    file, errno, strerror(errno)
                );
            } else {
                /* too many failed retries, give up */
                AXL_ERR("Giving up write of file %s errno=%d %s",
                    file, errno, strerror(errno)
                );
                return -1;
            }
        }
    }
    return n;
}

/* fsync and close file */
int axl_close(const char* file, int fd)
{
    /* fsync first */
    if (fsync(fd) < 0) {
        /* print warning that fsync failed */
        AXL_DBG(2, "Failed to fsync file descriptor: %s errno=%d %s",
            file, errno, strerror(errno)
        );
    }

    /* now close the file */
    if (close(fd) != 0) {
        /* hit an error, print message */
        AXL_ERR("Closing file descriptor %d for file %s: errno=%d %s",
            fd, file, errno, strerror(errno)
        );
        return AXL_FAILURE;
    }

    return AXL_SUCCESS;
}

static void axl_stat_get_atimes(const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
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

static void axl_stat_get_mtimes (const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
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

static void axl_stat_get_ctimes (const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
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

int axl_meta_encode(const char* file, kvtree* meta)
{
    struct stat statbuf;
    int rc = lstat(file, &statbuf);
    if (rc == 0) {
        kvtree_util_set_unsigned_long(meta, "MODE", (unsigned long) statbuf.st_mode);
        kvtree_util_set_unsigned_long(meta, "UID",  (unsigned long) statbuf.st_uid);
        kvtree_util_set_unsigned_long(meta, "GID",  (unsigned long) statbuf.st_gid);
        kvtree_util_set_unsigned_long(meta, "SIZE", (unsigned long) statbuf.st_size);
  
        uint64_t secs, nsecs;
        axl_stat_get_atimes(&statbuf, &secs, &nsecs);
        kvtree_util_set_unsigned_long(meta, "ATIME_SECS",  (unsigned long) secs);
        kvtree_util_set_unsigned_long(meta, "ATIME_NSECS", (unsigned long) nsecs);
  
        axl_stat_get_ctimes(&statbuf, &secs, &nsecs);
        kvtree_util_set_unsigned_long(meta, "CTIME_SECS",  (unsigned long) secs);
        kvtree_util_set_unsigned_long(meta, "CTIME_NSECS", (unsigned long) nsecs);
  
        axl_stat_get_mtimes(&statbuf, &secs, &nsecs);
        kvtree_util_set_unsigned_long(meta, "MTIME_SECS",  (unsigned long) secs);
        kvtree_util_set_unsigned_long(meta, "MTIME_NSECS", (unsigned long) nsecs);
  
        return AXL_SUCCESS;
    }
    return AXL_FAILURE;
}

/*
 * Check if a file is the size we expect it to be.  Do this by looking at the
 * SIZE field in the file's metadata kvtree.
 *
 * Return AXL_SUCCESS if the file is the correct size, AXL_FAILURE otherwise.
 * If there is no SIZE field in the metadata kvtree, return AXL_FAILURE.
 */
int axl_check_file_size(const char* file, const kvtree* meta)
{
    unsigned long size;
    int rc = AXL_SUCCESS;
    if (kvtree_util_get_unsigned_long(meta, "SIZE", &size) == KVTREE_SUCCESS) {
        /* got a size field in the metadata, stat the file */
        struct stat statbuf;
        int stat_rc = lstat(file, &statbuf);
        if (stat_rc == 0) {
            /* stat succeeded, check that sizes match */
            if (size != statbuf.st_size) {
                /* file size is not correct */
                AXL_ERR("file `%s' size is %lu expected %lu",
                    file, (unsigned long) statbuf.st_size, size
                );
                rc = AXL_FAILURE;
            }
        } else {
            /* failed to stat file */
            AXL_ERR("stat(%s) failed: errno=%d %s",
                file, errno, strerror(errno)
            );
            rc = AXL_FAILURE;
        }
    }

    return rc;
}

/*
 * For a given file, apply the metadata (like permission bits) that are set
 * in the file's metadata kvtree.
 */
int axl_meta_apply(const char* file, const kvtree* meta)
{
    int rc = AXL_SUCCESS;
  
    /* set permission bits on file */
    unsigned long mode_val;
    if (kvtree_util_get_unsigned_long(meta, "MODE", &mode_val) == KVTREE_SUCCESS) {
        mode_t mode = (mode_t) mode_val;
  
        /* TODO: mask some bits here */
  
        int chmod_rc = chmod(file, mode);
        if (chmod_rc != 0) {
            /* failed to set permissions */
            AXL_ERR("chmod(%s) failed: errno=%d %s",
                file, errno, strerror(errno)
            );
            rc = AXL_FAILURE;
        }
    }
  
    /* set uid and gid on file */
    unsigned long uid_val = -1;
    unsigned long gid_val = -1;
    kvtree_util_get_unsigned_long(meta, "UID", &uid_val);
    kvtree_util_get_unsigned_long(meta, "GID", &gid_val);
    if (uid_val != -1 || gid_val != -1) {
        /* got a uid or gid value, try to set them */
        int chown_rc = chown(file, (uid_t) uid_val, (gid_t) gid_val);
        if (chown_rc != 0) {
            /* failed to set uid and gid */
            AXL_ERR("chown(%s, %lu, %lu) failed: errno=%d %s",
                file, uid_val, gid_val, errno, strerror(errno)
            );
            rc = AXL_FAILURE;
        }
    }
  
    /* set timestamps on file as last step */
    unsigned long atime_secs  = 0;
    unsigned long atime_nsecs = 0;
    kvtree_util_get_unsigned_long(meta, "ATIME_SECS",  &atime_secs);
    kvtree_util_get_unsigned_long(meta, "ATIME_NSECS", &atime_nsecs);
  
    unsigned long mtime_secs  = 0;
    unsigned long mtime_nsecs = 0;
    kvtree_util_get_unsigned_long(meta, "MTIME_SECS",  &mtime_secs);
    kvtree_util_get_unsigned_long(meta, "MTIME_NSECS", &mtime_nsecs);
  
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
            axl_err("Failed to change timestamps on `%s' utimensat() errno=%d %s",
                file, errno, strerror(errno)
            );
            rc = AXL_FAILURE;
        }
    }
  
    return rc;
}

/*
 * Get env var AXL_DEBUG_PAUSE_AFTER and convert it to an unsigned long.
 * This tells AXL to pause a transfer after AXL_DEBUG_PAUSE_AFTER bytes have
 * been copied.  It's only used for test cases (like testing resuming a
 * transfer).
 *
 * If the var is not set, return ULONG_MAX.
 */
static unsigned long axl_debug_pause_after(void)
{
    char* env = getenv("AXL_DEBUG_PAUSE_AFTER");
    if (! env) {
        return ULONG_MAX;
    }
    return strtoul(env, 0, 10);
}

/* TODO: could perhaps use O_DIRECT here as an optimization */
/* TODO: could apply compression/decompression here */
/* copy src_file (full path) to dest_path and return new full path in dest_file */
int axl_file_copy(
    const char* src_file,
    const char* dst_file,
    unsigned long buf_size,
    int resume)
{
    /* check that we got something for a source file */
    if (src_file == NULL || strcmp(src_file, "") == 0) {
        AXL_ERR("Invalid source file");
        return AXL_FAILURE;
    }

    /* check that we got something for a destination file */
    if (dst_file == NULL || strcmp(dst_file, "") == 0) {
        AXL_ERR("Invalid destination file");
        return AXL_FAILURE;
    }

    int rc = AXL_SUCCESS;

    /* open src_file for reading */
    int src_fd = axl_open(src_file, O_RDONLY);
    if (src_fd < 0) {
        AXL_ERR("Opening file to copy: axl_open(%s) errno=%d %s",
            src_file, errno, strerror(errno)
        );
        return AXL_FAILURE;
    }

    mode_t mode_file = axl_getmode(1, 1, 0);

    int flags;
    if (resume) {
        flags = O_WRONLY | O_CREAT | O_APPEND;
    } else {
        flags = O_WRONLY | O_CREAT | O_TRUNC;
    }

    /* open dest_file for writing */
    int dst_fd = axl_open(dst_file, flags, mode_file);
    if (dst_fd < 0) {
        AXL_ERR("Opening file for writing: axl_open(%s) errno=%d %s",
            dst_file, errno, strerror(errno)
        );
        axl_close(src_file, src_fd);
        return AXL_FAILURE;
    }

#if !defined(__APPLE__)
    /* TODO:
       posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
       that tells the kernel that you don't ever need the pages
       from the file again, and it won't bother keeping them in the page cache.
    */
    posix_fadvise(src_fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);
    posix_fadvise(dst_fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);
#endif

    /* allocate buffer to read in file chunks */
    char* buf = (char*) malloc(buf_size);
    if (buf == NULL) {
        AXL_ERR("Allocating memory: malloc(%lu) errno=%d %s",
            buf_size, errno, strerror(errno)
        );
        axl_close(dst_file, dst_fd);
        axl_close(src_file, src_fd);
        return AXL_FAILURE;
    }

    /* Resume the transfer to our destination file where we left off */
    if (resume) {
        /* Seek to the end of our destination file, while recording offset */
        off_t start_offset = lseek(dst_fd, 0, SEEK_END);

        /* Set the src file position to the start_offset of dst file */
        if (lseek(src_fd, start_offset, SEEK_SET) != start_offset) {
            AXL_ERR("Couldn't seek src file errno=%d %s",
                errno, strerror(errno)
            );
            axl_free(buf);
            axl_close(dst_file, dst_fd);
            axl_close(src_file, src_fd);
            return AXL_FAILURE;
        }
    }

    unsigned long total_copied = 0;
    unsigned long pause_after = axl_debug_pause_after();

    /* write chunks */
    int copying = 1;
    while (copying) {
        /* attempt to read buf_size bytes from file */
        int nread = axl_read_attempt(src_file, src_fd, buf, buf_size);

        /* if we read some bytes, write them out */
        if (nread > 0) {
            /* write our nread bytes out */
            int nwrite = axl_write_attempt(dst_file, dst_fd, buf, nread);

            /* check for a write error or a short write */
            if (nwrite != nread) {
                /* write had a problem, stop copying and return an error */
                copying = 0;
                rc = AXL_FAILURE;
            }
        }

        /* assume a short read means we hit the end of the file */
        if (nread < buf_size) {
            copying = 0;
        }

        /* check for a read error, stop copying and return an error */
        if (nread < 0) {
            /* read had a problem, stop copying and return an error */
            copying = 0;
            rc = AXL_FAILURE;
        }

        /* Possibly pause our transfer for unit tests */
        total_copied += nread;
        while (pause_after != ULONG_MAX && total_copied >= pause_after);
    }

    /* free buffer */
    axl_free(&buf);

    /* close source and destination files */
    if (axl_close(dst_file, dst_fd) != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }
    if (axl_close(src_file, src_fd) != AXL_SUCCESS) {
        rc = AXL_FAILURE;
    }

    if (rc != AXL_SUCCESS) {
        /* unlink the file if the copy failed */
        axl_file_unlink(dst_file);
    }

    return rc;
}

/* opens, reads, and computes the crc32 value for the given filename */
int axl_crc32(const char* filename, uLong* crc)
{
    /* check that we got a variable to write our answer to */
    if (crc == NULL) {
        return AXL_FAILURE;
    }

    /* initialize our crc value */
    *crc = crc32(0L, Z_NULL, 0);

    /* open the file for reading */
    int fd = axl_open(filename, O_RDONLY);
    if (fd < 0) {
        AXL_DBG(1, "Failed to open file to compute crc: %s errno=%d",
            filename, errno
        );
        return AXL_FAILURE;
    }

    /* read the file data in and compute its crc32 */
    int nread = 0;
    unsigned long buffer_size = 1024*1024;
    char buf[buffer_size];
    do {
        nread = axl_read(filename, fd, buf, buffer_size);
        if (nread > 0) {
            *crc = crc32(*crc, (const Bytef*) buf, (uInt) nread);
        }
    } while (nread == buffer_size);

    /* if we got an error, don't print anything and bailout */
    if (nread < 0) {
        AXL_DBG(1, "Error while reading file to compute crc: %s", filename);
        close(fd);
        return AXL_FAILURE;
    }

    /* close the file */
    axl_close(filename, fd);

    return AXL_SUCCESS;
}

/* given a filename, return number of bytes in file */
unsigned long axl_file_size(const char* file)
{
    /* get file size in bytes */
    unsigned long bytes = 0;
    struct stat stat_buf;
    int stat_rc = stat(file, &stat_buf);
    if (stat_rc == 0) {
        bytes = stat_buf.st_size;
    }
    return bytes;
}
