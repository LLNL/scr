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

/* Implements a reliable open/read/write/close interface via open and close.
 * Implements directory manipulation functions. */

#include "scr_conf.h"
#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_util.h"

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

/* compute crc32 */
#include <zlib.h>

/*
=========================================
File compression functions
=========================================
*/

#define SCR_FILE_MAGIC                (0x951fc3f5)
#define SCR_FILE_TYPE_COMPRESSED      (2)
#define SCR_FILE_VERSION_COMPRESSED_1 (1)

#define SCR_FILE_COMPRESSED_HEADER_SIZE (44)
/* (4) uint32_t magic number
 * (2) uint16_t type
 * (2) uint16_t type version
 *
 * (8) uint64_t header size (this value includes the block table)
 * (8) uint64_t file size
 * (8) uint64_t block size
 * (8) uint64_t number of blocks
 *
 * <variable length block table> (excluded from SCR_FILE_COMPRESSED_HEADER_SIZE constant)
 *
 * (4) uint32_t header crc (from first byte of magic number to last byte of block table) */

/* compress insize bytes from in inbuf and store in outbuf which has
 * up to outsize bytes available, return number written in outwritten */
static int scr_compress_zlib(
  int level,          /* set compression level 0=none, 9=max */
  const void* inbuf,
  size_t insize,
  void* outbuf,
  size_t outsize,
  size_t* outwritten)
{
  int rc = SCR_SUCCESS;

  /* initialize output parameters */
  *outwritten = 0;

  /* initialize compression stream */
  z_stream strm;
  strm.zalloc = Z_NULL;
  strm.zfree  = Z_NULL;
  strm.opaque = Z_NULL;
  int ret = deflateInit(&strm, level);
  if (ret != Z_OK) {
    rc = SCR_FAILURE;
    return rc;
  }

  /* compress data */
  size_t written = 0;
  strm.avail_in  = insize;
  strm.next_in   = (void*)inbuf;
  strm.avail_out = outsize;
  strm.next_out  = outbuf;
  do {
    ret = deflate(&strm, Z_FINISH);
    if (ret == Z_OK || ret == Z_BUF_ERROR || ret == Z_STREAM_END) {
      /* compute number of bytes written by this call to deflate */
      written = outsize - strm.avail_out;
    } else {
      /* hit an error of some sort */
      scr_err("Error during compression (ret=%d) @ %s:%d",
        ret, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  } while (strm.avail_in !=0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

  /* check that we compressed the entire block */
  if (strm.avail_in != 0 || ret != Z_STREAM_END) {
    scr_err("Failed to compress @ %s:%d",
      __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* finalize the compression stream */
  deflateEnd(&strm);

  /* report number of bytes written to outbuf */
  *outwritten = written;

  return SCR_SUCCESS;
}

/* compress insize bytes from in inbuf and store in outbuf which has
 * up to outsize bytes available, return number written in outwritten */
static int scr_uncompress_zlib(
  const void* inbuf,
  size_t insize,
  void* outbuf,
  size_t outsize,
  size_t* outwritten)
{
  /* initialize output parameters */
  *outwritten = 0;

  /* initialize decompression stream */
  z_stream strm;
  strm.zalloc   = Z_NULL;
  strm.zfree    = Z_NULL;
  strm.opaque   = Z_NULL;
  strm.avail_in = 0;
  strm.next_in  = Z_NULL;
  int ret = inflateInit(&strm);
  if (ret != Z_OK) {
    scr_err("Failed to initialize decompression stream (ret=%d) @ %s:%d",
      ret, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* uncompress data */
  size_t written = 0;
  strm.avail_in  = insize;
  strm.next_in   = (void*)inbuf;
  strm.avail_out = outsize;
  strm.next_out  = outbuf;
  do {
    ret = inflate(&strm, Z_NO_FLUSH);
    if (ret == Z_NEED_DICT ||
        ret == Z_DATA_ERROR ||
        ret == Z_MEM_ERROR ||
        ret == Z_STREAM_ERROR)
    {
      /* hit an error of some sort */
      scr_err("Error during decompression (ret=%d) @ %s:%d",
        ret, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    } else {
      /* compute number of uncompressed bytes written so far */
      written = outsize - strm.avail_out;
    }
  } while (strm.avail_in != 0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

  /* check that we uncompressed the entire block */
  if (strm.avail_in != 0 || ret != Z_STREAM_END) {
    scr_err("Failed to decompress @ %s:%d",
      __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* finalize the compression stream */
  inflateEnd(&strm);

  /* report number of bytes written */
  *outwritten = written;

  return rc;
}

/* compress the specified buffer and return it in a newly allocated buffer,
 * returns SCR_SUCCESS if successful, in which case, caller must free buffer */
int scr_compress_buf(const void* inbuf, size_t insize, void** outbuf, size_t* outsize)
{
  /* set outbuf to NULL and outsize to 0 */
  *outbuf  = NULL;
  *outsize = 0;

  /* set compression level 0=none, 9=max */
  int compression_level = Z_DEFAULT_COMPRESSION;
//  int compression_level = level;

  unsigned long block_size = (unsigned long) insize;

  /* determine the number of blocks that we'll write */
  unsigned long num_blocks = 1;

  /* initialize compression stream */
  z_stream strm;
  strm.zalloc = Z_NULL;
  strm.zfree  = Z_NULL;
  strm.opaque = Z_NULL;
  int ret = deflateInit(&strm, compression_level);
  if (ret != Z_OK) {
    return SCR_FAILURE;
  }

  /* compute the size of the header */
  unsigned long header_size =
    SCR_FILE_COMPRESSED_HEADER_SIZE +
    num_blocks * (2 * sizeof(uint64_t) + 2 * sizeof(uint32_t));

  /* determine upper bound of compressed data */
  uLong bound_size = deflateBound(&strm, (uLong) block_size);

  /* allocate buffer to write compressed data into */
  size_t total_size = ((size_t) header_size) + ((size_t) bound_size);
  void* buf = malloc(total_size);
  if (buf == NULL) {
    scr_abort(-1, "Allocating compress buffer malloc(%ld) errno=%d %s @ %s:%d",
      total_size, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  int rc = SCR_SUCCESS;

  /* write the SCR file magic number, file type, and version number */
  void* header = buf;
  size_t header_offset = 0;
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) SCR_FILE_MAGIC);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_TYPE_COMPRESSED);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_VERSION_COMPRESSED_1);

  /* write the size of the header, the original file size, block size, and number of blocks */
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) header_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) insize);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) num_blocks);

  /* read block from source file, compress, write to destination file */
  unsigned long block_offset_cmp = 0;

  /* record size of compressed block, crc of compressed block */
  unsigned long block_size_cmp = 0;
  uLong crc_cmp = crc32(0L, Z_NULL, 0);

  /* compute crc for original block */
  uLong crc_orig = crc32(0L, Z_NULL, 0);
  crc_orig = crc32(crc_orig, (const Bytef*) inbuf, (uInt) insize);

  /* compress data */
  char* buf_dst = (char*)buf + header_size;
  size_t have = 0;
  strm.avail_in  = insize;
  strm.next_in   = (void*)inbuf;
  strm.avail_out = bound_size;
  strm.next_out  = buf_dst;
  do {
    ret = deflate(&strm, Z_FINISH);
    if (ret == Z_OK || ret == Z_BUF_ERROR || ret == Z_STREAM_END) {
      /* compute number of bytes written by this call to deflate */
      have = bound_size - strm.avail_out;
    } else {
      /* hit an error of some sort */
      scr_err("Error during compression (ret=%d) @ %s:%d",
        ret, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  } while (strm.avail_in !=0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

  /* write data */
  if (have > 0 && rc == SCR_SUCCESS) {
    /* compute crc of compressed block */
    crc_cmp = crc32(crc_cmp, (const Bytef*) buf_dst, (uInt) have);

    /* add count to our total compressed block size */
    block_size_cmp += have;
  }

  /* check that we compressed the entire block */
  if (strm.avail_in != 0 || ret != Z_STREAM_END) {
    scr_err("Failed to compress @ %s:%d",
      __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* finalize the compression stream */
  deflateEnd(&strm);

  /* TODO: handle the case where compressed size is larger than original size */
  if (block_size_cmp > block_size) {
    scr_abort(-1, "Compressed size is larger than original size @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* add entry for block in header: length, crc cmp, crc orig */
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_offset_cmp);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size_cmp);
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_cmp);
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_orig);
  block_offset_cmp += block_size_cmp;

  /* compute crc over length of the header and write it to header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) header_offset);
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc);

  /* free our buffers */
  if (rc == SCR_SUCCESS) {
    /* set output parameters */
    *outbuf  = buf;
    *outsize = (size_t) (header_size + block_offset_cmp);
  } else {
    /* free the buffer if we weren't successful uncompressing data */
    scr_free(&buf);
  }

  return rc;
}

/* uncompress the specified buffer and return it in a newly allocated buffer,
 * returns SCR_SUCCESS if successful, in which case, caller must free buffer */
int scr_uncompress_buf(const void* inbuf, size_t insize, void** outbuf, size_t* outsize)
{
  /* set outbuf to NULL and outsize to 0 */
  *outbuf  = NULL;
  *outsize = 0;

  size_t size = 0;

  /* unpack magic number, the type, and the version */
  uint32_t magic;
  uint16_t type, version;
  scr_unpack_uint32_t(inbuf, insize, &size, &magic);
  scr_unpack_uint16_t(inbuf, insize, &size, &type);
  scr_unpack_uint16_t(inbuf, insize, &size, &version);

  /* check the magic number, the type, and the version */
  if (magic   != SCR_FILE_MAGIC ||
      type    != SCR_FILE_TYPE_COMPRESSED ||
      version != SCR_FILE_VERSION_COMPRESSED_1)
  {
    scr_err("File type does not match values for a compressed file @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read size of header, file size, block size, and number of blocks */
  uint64_t header_size, datasize, block_size, num_blocks;
  scr_unpack_uint64_t(inbuf, insize, &size, &header_size);
  scr_unpack_uint64_t(inbuf, insize, &size, &datasize);
  scr_unpack_uint64_t(inbuf, insize, &size, &block_size);
  scr_unpack_uint64_t(inbuf, insize, &size, &num_blocks);

  /* point to header */
  const void* header = inbuf;

  /* get crc for header */
  uint32_t crc_header;
  size_t header_offset = header_size - sizeof(uint32_t);
  scr_unpack_uint32_t(header, header_size, &header_offset, &crc_header);

  /* compute crc over length of the header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) (header_size - sizeof(uint32_t)));

  /* check that crc values match */
  if ((uLong) crc_header != crc) {
    scr_err("CRC32 mismatch detected in header @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* set header offset to point to entry for first block */
  header_offset = 
    sizeof(uint32_t)        /* magic */
    + 2 * sizeof(uint16_t)  /* type and version */
    + 4 * sizeof(uint64_t); /* header size, file size, block size, num blocks */

  /* TODO: handle case where num_blocks > 1 */
  if (num_blocks != 1) {
    scr_abort(-1, "Cannot currently uncompress more than one block @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* read entry for block from header: length, crc cmp, crc orig */
  uint64_t block_offset_cmp, block_size_cmp;
  uint32_t file_crc_cmp, file_crc_orig;
  scr_unpack_uint64_t(header, header_size, &header_offset, &block_offset_cmp);
  scr_unpack_uint64_t(header, header_size, &header_offset, &block_size_cmp);
  scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_cmp);
  scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_orig);

  /* get pointer to first byte of compressed data */
  char* buf_src = (char*)header +
    SCR_FILE_COMPRESSED_HEADER_SIZE +
    num_blocks * (2 * sizeof(uint64_t) + 2 * sizeof(uint32_t));

  /* compute crc for compressed block */
  uLong crc_cmp = crc32(0L, Z_NULL, 0);
  crc_cmp = crc32(crc_cmp, (const Bytef*) buf_src, (uInt) block_size_cmp);
  if (crc_cmp != file_crc_cmp) {
    /* CRC failure on compressed data, don't both decompressing */
    scr_err("CRC32 mismatch detected in compressed block @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf = malloc((size_t) block_size);
  if (buf == NULL) {
    scr_abort(-1, "Allocating buffer to decompress data malloc(%ld) errno=%d %s @ %s:%d",
      block_size, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  /* decompress data */
  size_t written;
  int rc = scr_uncompress_zlib(buf_src, block_size_cmp, buf, block_size, &written);

  /* if we decompressed ok, check crc of uncompressed data */
  if (rc == SCR_SUCCESS && written > 0) {
    /* compute crc of uncompressed block */
    uLong crc_orig = crc32(0L, Z_NULL, 0);
    crc_orig = crc32(crc_orig, (const Bytef*) buf, (uInt) written);
    if (crc_orig != file_crc_orig) {
      scr_err("CRC32 mismatch detected in decompressed block @ %s:%d",
        __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  if (rc == SCR_SUCCESS) {
    /* set output parameters */
    *outbuf  = buf;
    *outsize = written;
  } else {
    /* free the buffer if we weren't successful uncompressing data */
    scr_free(&buf);
  }

  return rc;
}

/* compress the specified file using blocks of size block_size and store as file_dst */
int scr_compress_in_place(const char* file_src, const char* file_dst, unsigned long block_size, int level)
{
  /* set compression level 0=none, 9=max */
//  int compression_level = Z_DEFAULT_COMPRESSION;
  int compression_level = level;

  /* check that we have valid file names */
  if (file_src == NULL || file_dst == NULL) {
    scr_err("NULL filename @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the source file exists and that we can read and write to it */
  if (access(file_src, F_OK | R_OK | W_OK) != 0) {
    scr_err("File %s does not exist or does not have read/write permission @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the page size (used to align buffers) */
  int page_size = getpagesize();
  if (page_size <= 0) {
    scr_err("Call to getpagesize failed when compressing %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the size of the file */
  unsigned long filesize = scr_file_size(file_src);

  /* determine the number of blocks that we'll write */
  unsigned long num_blocks = filesize / block_size;
  if (num_blocks * block_size < filesize) {
    num_blocks++;
  }

  /* compute the size of the header */
  unsigned long header_size = SCR_FILE_COMPRESSED_HEADER_SIZE + num_blocks * (2 * sizeof(uint64_t) + 2 * sizeof(uint32_t));

  /* allocate buffer to hold file header */
  void* header = malloc(header_size);
  if (header == NULL) {
    scr_err("Allocating header buffer when compressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, header_size, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf_src = scr_align_malloc((size_t) block_size, page_size);
  if (buf_src == NULL) {
    scr_err("Allocating source buffer when compressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, block_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&header);
    return SCR_FAILURE;
  }

  /* allocate buffer to write compressed data into */
  void* buf_dst = scr_align_malloc((size_t) block_size, page_size);
  if (buf_dst == NULL) {
    scr_err("Allocating compress buffer when compressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, block_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&buf_src);
    scr_free(&header);
    return SCR_FAILURE;
  }

  /* open original file for read/write access */
  int fd_src = scr_open(file_src, O_RDWR);
  if (fd_src < 0) {
    scr_err("Opening file: %s errno=%d %s @ %s:%d",
            file_src, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&buf_dst);
    scr_free(&buf_src);
    scr_free(&header);
    return SCR_FAILURE;
  }

  /* these pointers will track our location within the file,
   * we must make sure that we never overrun the original data when compressing */
  off_t pos_src = 0;
  off_t pos_dst = 0;

  int rc = SCR_SUCCESS;

  /* write the SCR file magic number, file type, and version number */
  size_t header_offset = 0;
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) SCR_FILE_MAGIC);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_TYPE_COMPRESSED);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_VERSION_COMPRESSED_1);

  /* write the size of the header, the original file size, block size, and number of blocks */
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) header_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) filesize);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) num_blocks);

  /* seek to end of header */
  pos_dst = header_size;

  /* read block from source file, compress, write to destination file */
  unsigned long block_offset_cmp = 0;
  int compressing = 1;
  while (compressing && rc == SCR_SUCCESS) {
    /* seek to current location for reading */
    if (lseek(fd_src, (off_t) pos_src, SEEK_SET) == (off_t) -1) {
      scr_err("Seek to read position failed in %s @ %s:%d",
              file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* read a block in from the file */
    ssize_t nread = scr_read(file_src, fd_src, buf_src, block_size);

    /* compress data and write it to file */
    if (nread > 0) {
      /* update our read position */
      pos_src += nread;

      /* record size of compressed block,
       * crc of compressed block, and crc of original block */
      unsigned long block_size_cmp = 0;
      uLong crc_cmp  = crc32(0L, Z_NULL, 0);
      uLong crc_orig = crc32(0L, Z_NULL, 0);

      /* compute crc for block */
      crc_orig = crc32(crc_orig, (const Bytef*) buf_src, (uInt) nread);

      /* initialize compression stream */
      z_stream strm;
      strm.zalloc = Z_NULL;
      strm.zfree  = Z_NULL;
      strm.opaque = Z_NULL;
      int ret = deflateInit(&strm, compression_level);
      if (ret != Z_OK) {
        rc = SCR_FAILURE;
      }

      /* compress data */
      strm.avail_in = nread;
      strm.next_in  = buf_src;
      do {
        size_t have = 0;
        strm.avail_out = block_size;
        strm.next_out  = buf_dst;
        do {
          ret = deflate(&strm, Z_FINISH);
          if (ret == Z_OK || ret == Z_BUF_ERROR || ret == Z_STREAM_END) {
            /* compute number of bytes written by this call to deflate */
            have = block_size - strm.avail_out;
          } else {
            /* hit an error of some sort */
            scr_err("Error during compression in %s (ret=%d) @ %s:%d",
                    file_src, ret, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }
        } while (strm.avail_in !=0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

        /* TODO: if compression produces very small blocks, this will be inefficient,
         * would be better to use a buffered write like fwrite here */

        /* write data */
        if (have > 0 && rc == SCR_SUCCESS) {
          /* compute crc of compressed block */
          crc_cmp = crc32(crc_cmp, (const Bytef*) buf_dst, (uInt) have);

          /* check that we won't overrun our read position when we write out this data */
          off_t pos_end = pos_dst + have;
          if (pos_end > pos_src && pos_src != filesize) {
            /* TODO: unwind what compression we have done if any,
             * for now we just make this a fatal error */
            scr_err("Failed to compress file in place %s @ %s:%d",
                    file_src, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* seek to correct location in file to write data */
          if (lseek(fd_src, (off_t) pos_dst, SEEK_SET) == (off_t) -1) {
            scr_err("Seek to write position failed in %s @ %s:%d",
                    file_src, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* write compressed data to file */
          ssize_t nwrite = scr_write(file_src, fd_src, buf_dst, have);
          if (nwrite != have) {
            scr_err("Error writing compressed file %s @ %s:%d",
                    file_src, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* update our write position */
          if (nwrite > 0) {
            pos_dst += nwrite;
          }

          /* add count to our total compressed block size */
          block_size_cmp += have;
        }
      } while ((ret == Z_OK || ret == Z_BUF_ERROR) && rc == SCR_SUCCESS); /* strm.avail_out == 0 */

      /* check that we compressed the entire block */
      if (strm.avail_in != 0 || ret != Z_STREAM_END) {
        scr_err("Failed to compress file %s @ %s:%d",
                file_src, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* finalize the compression stream */
      deflateEnd(&strm);

      /* add entry for block in header: length, crc cmp, crc orig */
      scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_offset_cmp);
      scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size_cmp);
      scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_cmp);
      scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_orig);
      block_offset_cmp += block_size_cmp;
    }

    /* check whether we've read all of the input file */
    if (nread < block_size) {
      compressing = 0;
    }
  }

  /* compute crc over length of the header and write it to header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) header_offset);
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc);

  /* seek to beginning of file */
  if (lseek(fd_src, (off_t) 0, SEEK_SET) == (off_t) -1) {
    scr_err("Seek to beginning of header failed in %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* write header to file */
  ssize_t nwrite_header = scr_write(file_src, fd_src, header, header_size);
  if (nwrite_header != header_size) {
    scr_err("Failed to write header to file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* close file */
  scr_close(file_src, fd_src);

  /* truncate file */
  truncate(file_src, pos_dst);

  /* rename file */
  rename(file_src, file_dst);

  /* free our buffers */
  scr_align_free(&buf_dst);
  scr_align_free(&buf_src);
  scr_free(&header);

  return rc;
}

/* compress the specified file using blocks of size block_size and store as file_dst */
int scr_compress(const char* file_src, const char* file_dst, unsigned long block_size, int level)
{
  /* set compression level 0=none, 9=max */
//  int compression_level = Z_DEFAULT_COMPRESSION;
  int compression_level = level;

  /* check that we have valid file names */
  if (file_src == NULL || file_dst == NULL) {
    scr_err("NULL filename @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the source file exists and that we can read it */
  if (access(file_src, F_OK | R_OK) != 0) {
    scr_err("File %s does not exist or does not have read permission @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the page size (used to align buffers) */
  int page_size = getpagesize();
  if (page_size <= 0) {
    scr_err("Call to getpagesize failed when compressing %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the size of the file */
  unsigned long filesize = scr_file_size(file_src);

  /* determine the number of blocks that we'll write */
  unsigned long num_blocks = filesize / block_size;
  if (num_blocks * block_size < filesize) {
    num_blocks++;
  }

  /* compute the size of the header */
  unsigned long header_size = SCR_FILE_COMPRESSED_HEADER_SIZE + num_blocks * (2 * sizeof(uint64_t) + 2 * sizeof(uint32_t));

  /* allocate buffer to hold file header */
  void* header = malloc(header_size);
  if (header == NULL) {
    scr_err("Allocating header buffer when compressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, header_size, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf_src = scr_align_malloc((size_t) block_size, page_size);
  if (buf_src == NULL) {
    scr_err("Allocating source buffer when compressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, block_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&header);
    return SCR_FAILURE;
  }

  /* allocate buffer to write compressed data into */
  void* buf_dst = scr_align_malloc((size_t) block_size, page_size);
  if (buf_dst == NULL) {
    scr_err("Allocating compress buffer when compressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, block_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&buf_src);
    scr_free(&header);
    return SCR_FAILURE;
  }

  /* open original file */
  int fd_src = scr_open(file_src, O_RDONLY);
  if (fd_src < 0) {
    scr_err("Opening file for reading: %s errno=%d %s @ %s:%d",
            file_src, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&buf_dst);
    scr_free(&buf_src);
    scr_free(&header);
    return SCR_FAILURE;
  }

  /* open compressed file for writing */
  mode_t mode_file = scr_getmode(1, 1, 0);
  int fd_dst = scr_open(file_dst, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (fd_dst < 0) {
    scr_err("Opening file for writing: %s errno=%d %s @ %s:%d",
            file_dst, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    scr_free(&buf_dst);
    scr_free(&buf_src);
    scr_free(&header);
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* write the SCR file magic number, file type, and version number */
  size_t header_offset = 0;
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) SCR_FILE_MAGIC);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_TYPE_COMPRESSED);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_VERSION_COMPRESSED_1);

  /* write the size of the header, the original file size, block size, and number of blocks */
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) header_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) filesize);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) num_blocks);

  /* seek to end of header */
  if (lseek(fd_dst, (off_t) header_size, SEEK_SET) == (off_t) -1) {
    scr_err("Seek to end of header failed in %s @ %s:%d",
            file_dst, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* read block from source file, compress, write to destination file */
  unsigned long block_offset_cmp = 0;
  int compressing = 1;
  while (compressing && rc == SCR_SUCCESS) {
    /* read a block in from the file */
    ssize_t nread = scr_read(file_src, fd_src, buf_src, block_size);

    /* compress data and write it to file */
    if (nread > 0) {
      /* record size of compressed block,
       * crc of compressed block, and crc of original block */
      unsigned long block_size_cmp = 0;
      uLong crc_cmp  = crc32(0L, Z_NULL, 0);
      uLong crc_orig = crc32(0L, Z_NULL, 0);

      /* compute crc for block */
      crc_orig = crc32(crc_orig, (const Bytef*) buf_src, (uInt) nread);

      /* initialize compression stream */
      z_stream strm;
      strm.zalloc = Z_NULL;
      strm.zfree  = Z_NULL;
      strm.opaque = Z_NULL;
      int ret = deflateInit(&strm, compression_level);
      if (ret != Z_OK) {
        rc = SCR_FAILURE;
      }

      /* compress data */
      strm.avail_in = nread;
      strm.next_in  = buf_src;
      do {
        size_t have = 0;
        strm.avail_out = block_size;
        strm.next_out  = buf_dst;
        do {
          ret = deflate(&strm, Z_FINISH);
          if (ret == Z_OK || ret == Z_BUF_ERROR || ret == Z_STREAM_END) {
            /* compute number of bytes written by this call to deflate */
            have = block_size - strm.avail_out;
          } else {
            /* hit an error of some sort */
            scr_err("Error during compression in %s (ret=%d) @ %s:%d",
                    file_src, ret, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }
        } while (strm.avail_in !=0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

        /* TODO: if compression produces very small blocks, this will be inefficient,
         * would be better to use a buffered write like fwrite here */

        /* write data */
        if (have > 0 && rc == SCR_SUCCESS) {
          /* compute crc of compressed block */
          crc_cmp = crc32(crc_cmp, (const Bytef*) buf_dst, (uInt) have);

          /* write compressed data to file */
          ssize_t nwrite = scr_write(file_dst, fd_dst, buf_dst, have);
          if (nwrite != have) {
            scr_err("Error writing compressed file %s @ %s:%d",
                    file_dst, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* add count to our total compressed block size */
          block_size_cmp += have;
        }
      } while ((ret == Z_OK || ret == Z_BUF_ERROR) && rc == SCR_SUCCESS); /* strm.avail_out == 0 */

      /* check that we compressed the entire block */
      if (strm.avail_in != 0 || ret != Z_STREAM_END) {
        scr_err("Failed to compress file %s @ %s:%d",
                file_src, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* finalize the compression stream */
      deflateEnd(&strm);

      /* add entry for block in header: length, crc cmp, crc orig */
      scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_offset_cmp);
      scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size_cmp);
      scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_cmp);
      scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_orig);
      block_offset_cmp += block_size_cmp;
    }

    /* check whether we've read all of the input file */
    if (nread < block_size) {
      compressing = 0;
    }
  }

  /* compute crc over length of the header and write it to header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) header_offset);
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc);

  /* seek to beginning of file */
  if (lseek(fd_dst, (off_t) 0, SEEK_SET) == (off_t) -1) {
    scr_err("Seek to beginning of header failed in %s @ %s:%d",
            file_dst, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* write header to file */
  ssize_t nwrite_header = scr_write(file_dst, fd_dst, header, header_size);
  if (nwrite_header != header_size) {
    scr_err("Failed to write header to file %s @ %s:%d",
            file_dst, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* close files */
  scr_close(file_src, fd_src);
  scr_close(file_dst, fd_dst);

  /* TODO: truncate file */

  /* TODO: rename file */

  /* free our buffers */
  scr_align_free(&buf_dst);
  scr_align_free(&buf_src);
  scr_free(&header);

  return rc;
}

/* uncompress the specified file and store as file_dst */
int scr_uncompress_in_place(const char* file_src, const char* file_dst)
{
  /* check that we have valid file names */
  if (file_src == NULL || file_dst == NULL) {
    scr_err("NULL filename @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the source file exists and that we can read and write to it */
  if (access(file_src, F_OK | R_OK | W_OK) != 0) {
    scr_err("File %s does not exist or does not have read/write permission @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the page size (used to align buffers) */
  int page_size = getpagesize();
  if (page_size <= 0) {
    scr_err("Call to getpagesize failed when decompressing %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open original file for reading and writing */
  int fd_src = scr_open(file_src, O_RDWR);
  if (fd_src < 0) {
    scr_err("Opening file for reading: %s errno=%d %s @ %s:%d",
            file_src, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read first few bytes of header */
  char buf[SCR_FILE_COMPRESSED_HEADER_SIZE];
  size_t nread = scr_read(file_src, fd_src, buf, sizeof(buf));
  if (nread < sizeof(buf)) {
    scr_err("Failed to read header from file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  size_t size = 0;

  /* unpack magic number, the type, and the version */
  uint32_t magic;
  uint16_t type, version;
  scr_unpack_uint32_t(buf, sizeof(buf), &size, &magic);
  scr_unpack_uint16_t(buf, sizeof(buf), &size, &type);
  scr_unpack_uint16_t(buf, sizeof(buf), &size, &version);

  /* check the magic number, the type, and the version */
  if (magic   != SCR_FILE_MAGIC ||
      type    != SCR_FILE_TYPE_COMPRESSED ||
      version != SCR_FILE_VERSION_COMPRESSED_1)
  {
    scr_err("File type does not match values for a compressed file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* read size of header, file size, block size, and number of blocks */
  uint64_t header_size, filesize, block_size, num_blocks;
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &header_size);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &filesize);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &block_size);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &num_blocks);

  /* allocate buffer to hold file header */
  void* header = malloc(header_size);
  if (header == NULL) {
    scr_err("Allocating header buffer when decompressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, header_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf_src = scr_align_malloc((size_t) block_size, page_size);
  if (buf_src == NULL) {
    scr_err("Allocating source buffer when decompressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, block_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&header);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* allocate buffer to write uncompressed data into */
  void* buf_dst = scr_align_malloc((size_t) block_size, page_size);
  if (buf_dst == NULL) {
    scr_err("Allocating compress buffer when decompressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, block_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&buf_src);
    scr_free(&header);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* these pointers will track our location within the file,
   * we must make sure that we never overrun the original data when compressing */
  off_t pos_src = 0;
  off_t pos_dst = 0;

  int rc = SCR_SUCCESS;

  /* seek back to start of file to read in full header */
  if (lseek(fd_src, 0, SEEK_SET) == (off_t) -1) {
    scr_err("Failed to seek to start of file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* read in full header, this time including block table */
  nread = scr_read(file_src, fd_src, header, header_size);
  if (nread < header_size) {
    scr_err("Failed to read in header from file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* get crc for header */
  uint32_t crc_header;
  size_t header_offset = header_size - sizeof(uint32_t);
  scr_unpack_uint32_t(header, header_size, &header_offset, &crc_header);

  /* compute crc over length of the header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) (header_size - sizeof(uint32_t)));

  /* check that crc values match */
  if ((uLong) crc_header != crc) {
    scr_err("CRC32 mismatch detected in header of %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* set pointer to end of block table */
  header_offset = sizeof(uint32_t) + 2 * sizeof(uint16_t) + 4 * sizeof(uint64_t) +
                  num_blocks * (2 * sizeof(uint64_t) + 2 * sizeof(uint32_t));

  /* read block from source file, compress, write to destination file */
  int block_count = 0;
  while (block_count < num_blocks && rc == SCR_SUCCESS) {
    /* back up one entry in the block table */
    header_offset -= 2 * sizeof(uint64_t) + 2 * sizeof(uint32_t);

    /* read entry for block from header: length, crc cmp, crc orig */
    uint64_t block_offset_cmp, block_size_cmp;
    uint32_t file_crc_cmp, file_crc_orig;
    scr_unpack_uint64_t(header, header_size, &header_offset, &block_offset_cmp);
    scr_unpack_uint64_t(header, header_size, &header_offset, &block_size_cmp);
    scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_cmp);
    scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_orig);

    /* set block pointer back to start of the block we just read */
    header_offset -= 2 * sizeof(uint64_t) + 2 * sizeof(uint32_t);

    /* initialize decompression stream */
    z_stream strm;
    strm.zalloc   = Z_NULL;
    strm.zfree    = Z_NULL;
    strm.opaque   = Z_NULL;
    strm.avail_in = 0;
    strm.next_in  = Z_NULL;
    int ret = inflateInit(&strm);
    if (ret != Z_OK) {
      scr_err("Failed to initialize decompression stream when processing %s (ret=%d) @ %s:%d",
              file_src, ret, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* read data, decompress, and write out */
    uint64_t total_read    = 0;
    uint64_t total_written = 0;
    uLong crc_cmp  = crc32(0L, Z_NULL, 0);
    uLong crc_orig = crc32(0L, Z_NULL, 0);
    while (total_read < block_size_cmp && rc == SCR_SUCCESS) {
      /* limit how much we read in */
      size_t count = block_size_cmp - total_read;
      if (count > block_size) {
        count = block_size;
      }

      /* TODO: these reads will be inefficient for very small compressed blocks
       * would be better to read with a buffered read like fread() here */

      /* seek to current location for reading */
      pos_src = header_size + block_offset_cmp + total_read;
      if (lseek(fd_src, (off_t) pos_src, SEEK_SET) == (off_t) -1) {
        scr_err("Seek to read position failed in %s @ %s:%d",
                file_src, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* read a block in from the file */
      ssize_t nread = scr_read(file_src, fd_src, buf_src, count);

      /* uncompress data and write it to file */
      if (nread > 0) {
        /* TODO: would be nice to handle this case, but we don't yet when doing
         * in place decompression */
        if (nread < block_size_cmp) {
          scr_err("Failed to read full compressed block from file %s @ %s:%d",
                  file_src, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }

        /* compute crc for compressed block */
        crc_cmp = crc32(crc_cmp, (const Bytef*) buf_src, (uInt) nread);

        /* uncompress data */
        strm.avail_in = nread;
        strm.next_in  = buf_src;
        do {
          /* record the number of uncompressed bytes */
          size_t have = 0;
          strm.avail_out = block_size;
          strm.next_out  = buf_dst;
          do {
            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_NEED_DICT ||
                ret == Z_DATA_ERROR ||
                ret == Z_MEM_ERROR ||
                ret == Z_STREAM_ERROR)
            {
              /* hit an error of some sort */
              scr_err("Error during decompression in %s (ret=%d) @ %s:%d",
                      file_src, ret, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            } else {
              /* compute number of uncompressed bytes written so far */
              have = block_size - strm.avail_out;
            }
          } while (strm.avail_in != 0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

          /* write data */
          if (have > 0 && rc == SCR_SUCCESS) {
            /* compute crc of uncompressed block */
            crc_orig = crc32(crc_orig, (const Bytef*) buf_dst, (uInt) have);

            /* determine byte location to start writing this data */
            pos_dst = (num_blocks - block_count - 1) * block_size + total_written;

            /* check that we don't clobber data we haven't yet read */
//            off_t pos_end = pos_src + block_size_cmp;
            off_t pos_end = pos_src; /* TODO: Here, we assume that we read the entire block starting at pos_src */
            if (pos_dst < pos_end && pos_src != header_size) {
              /* TODO: unwind what decompression we have done if any,
               * for now we just make this a fatal error */
              scr_err("Failed to decompress file in place %s @ %s:%d",
                      file_src, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }

            /* seek to current location for writing */
            if (lseek(fd_src, (off_t) pos_dst, SEEK_SET) == (off_t) -1) {
              scr_err("Seek to write position failed in %s @ %s:%d",
                      file_src, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }

            /* write uncompressed data to file */
            ssize_t nwrite = scr_write(file_src, fd_src, buf_dst, have);
            if (nwrite != have) {
              scr_err("Error writing to %s @ %s:%d",
                      file_src, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }

            /* update the number of uncompressed bytes we've written for this block */
            if (nwrite > 0) {
              total_written += nwrite;
            }
          }
        } while ((ret == Z_OK || ret == Z_BUF_ERROR) && rc == SCR_SUCCESS); /* strm.avail_out == 0 */

        /* check that we uncompressed the entire block */
        if (strm.avail_in != 0 || ret != Z_STREAM_END) {
          scr_err("Failed to decompress file %s @ %s:%d",
                  file_src, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }

        /* add to total that we've read */
        total_read += nread;
      }
    }

    /* done with this block, check crc values */
    if (crc_cmp != file_crc_cmp) {
      scr_err("CRC32 mismatch detected in compressed block #%d in %s @ %s:%d",
              block_count, file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
    if (crc_orig != file_crc_orig) {
      scr_err("CRC32 mismatch detected in decompressed block #%d in %s @ %s:%d",
              block_count, file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* finalize the compression stream */
    inflateEnd(&strm);

    /* increment our block count */
    block_count++;
  }

  /* close files */
  scr_close(file_src, fd_src);

  /* truncate file */
  truncate(file_src, filesize);

  /* rename file */
  rename(file_src, file_dst);

  /* free our buffers */
  scr_align_free(&buf_dst);
  scr_align_free(&buf_src);
  scr_free(&header);

  return rc;
}

/* uncompress the specified file and store as file_dst */
int scr_uncompress(const char* file_src, const char* file_dst)
{
  /* check that we have valid file names */
  if (file_src == NULL || file_dst == NULL) {
    scr_err("NULL filename @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the source file exists and that we can read it */
  if (access(file_src, F_OK | R_OK) != 0) {
    scr_err("File %s does not exist or does not have read permission @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the page size (used to align buffers) */
  int page_size = getpagesize();
  if (page_size <= 0) {
    scr_err("Call to getpagesize failed when decompressing %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open original file */
  int fd_src = scr_open(file_src, O_RDONLY);
  if (fd_src < 0) {
    scr_err("Opening file for reading: %s errno=%d %s @ %s:%d",
            file_src, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open compressed file for writing */
  mode_t mode_file = scr_getmode(1, 1, 0);
  int fd_dst = scr_open(file_dst, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (fd_dst < 0) {
    scr_err("Opening file for writing: %s errno=%d %s @ %s:%d",
            file_dst, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* read first few bytes of header */
  char buf[SCR_FILE_COMPRESSED_HEADER_SIZE];
  size_t nread = scr_read(file_src, fd_src, buf, sizeof(buf));
  if (nread < sizeof(buf)) {
    scr_err("Failed to read header from file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  size_t size = 0;

  /* unpack magic number, the type, and the version */
  uint32_t magic;
  uint16_t type, version;
  scr_unpack_uint32_t(buf, sizeof(buf), &size, &magic);
  scr_unpack_uint16_t(buf, sizeof(buf), &size, &type);
  scr_unpack_uint16_t(buf, sizeof(buf), &size, &version);

  /* check the magic number, the type, and the version */
  if (magic   != SCR_FILE_MAGIC ||
      type    != SCR_FILE_TYPE_COMPRESSED ||
      version != SCR_FILE_VERSION_COMPRESSED_1)
  {
    scr_err("File type does not match values for a compressed file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* read size of header, file size, block size, and number of blocks */
  uint64_t header_size, filesize, block_size, num_blocks;
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &header_size);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &filesize);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &block_size);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &num_blocks);

  /* allocate buffer to hold file header */
  void* header = malloc(header_size);
  if (header == NULL) {
    scr_err("Allocating header buffer when decompressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, header_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf_src = scr_align_malloc((size_t) block_size, page_size);
  if (buf_src == NULL) {
    scr_err("Allocating source buffer when decompressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, block_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&header);
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* allocate buffer to write uncompressed data into */
  void* buf_dst = scr_align_malloc((size_t) block_size, page_size);
  if (buf_dst == NULL) {
    scr_err("Allocating compress buffer when decompressing %s: malloc(%ld) errno=%d %s @ %s:%d",
            file_src, block_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&buf_src);
    scr_free(&header);
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* seek back to start of file to read in full header */
  if (lseek(fd_src, 0, SEEK_SET) == (off_t) -1) {
    scr_err("Failed to seek to start of file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* read in full header, this time including block table */
  nread = scr_read(file_src, fd_src, header, header_size);
  if (nread < header_size) {
    scr_err("Failed to read in header from file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* get crc for header */
  uint32_t crc_header;
  size_t header_offset = header_size - sizeof(uint32_t);
  scr_unpack_uint32_t(header, header_size, &header_offset, &crc_header);

  /* compute crc over length of the header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) (header_size - sizeof(uint32_t)));

  /* check that crc values match */
  if ((uLong) crc_header != crc) {
    scr_err("CRC32 mismatch detected in header of %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* set header offset to point to entry for first block */
  header_offset = sizeof(uint32_t) + 2 * sizeof(uint16_t) + 4 * sizeof(uint64_t);

  /* read block from source file, compress, write to destination file */
  int block_count = 0;
  while (block_count < num_blocks && rc == SCR_SUCCESS) {
    /* read entry for block from header: length, crc cmp, crc orig */
    uint64_t block_offset_cmp, block_size_cmp;
    uint32_t file_crc_cmp, file_crc_orig;
    scr_unpack_uint64_t(header, header_size, &header_offset, &block_offset_cmp);
    scr_unpack_uint64_t(header, header_size, &header_offset, &block_size_cmp);
    scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_cmp);
    scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_orig);

    /* initialize decompression stream */
    z_stream strm;
    strm.zalloc   = Z_NULL;
    strm.zfree    = Z_NULL;
    strm.opaque   = Z_NULL;
    strm.avail_in = 0;
    strm.next_in  = Z_NULL;
    int ret = inflateInit(&strm);
    if (ret != Z_OK) {
      scr_err("Failed to initialize decompression stream when processing %s (ret=%d) @ %s:%d",
              file_src, ret, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* read data, decompress, and write out */
    uint64_t total_read = 0;
    uLong crc_cmp  = crc32(0L, Z_NULL, 0);
    uLong crc_orig = crc32(0L, Z_NULL, 0);
    while (total_read < block_size_cmp && rc == SCR_SUCCESS) {
      /* limit how much we read in */
      size_t count = block_size_cmp - total_read;
      if (count > block_size) {
        count = block_size;
      }

      /* TODO: these reads will be inefficient for very small compressed blocks
       * would be better to read with a buffered read like fread() here */

      /* read a block in from the file */
      ssize_t nread = scr_read(file_src, fd_src, buf_src, count);

      /* uncompress data and write it to file */
      if (nread > 0) {
        /* compute crc for compressed block */
        crc_cmp = crc32(crc_cmp, (const Bytef*) buf_src, (uInt) nread);

        /* uncompress data */
        strm.avail_in = nread;
        strm.next_in  = buf_src;
        do {
          /* record the number of uncompressed bytes */
          size_t have = 0;
          strm.avail_out = block_size;
          strm.next_out  = buf_dst;
          do {
            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_NEED_DICT ||
                ret == Z_DATA_ERROR ||
                ret == Z_MEM_ERROR ||
                ret == Z_STREAM_ERROR)
            {
              /* hit an error of some sort */
              scr_err("Error during decompression in %s (ret=%d) @ %s:%d",
                      file_src, ret, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            } else {
              /* compute number of uncompressed bytes written so far */
              have = block_size - strm.avail_out;
            }
          } while (strm.avail_in != 0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

          /* write data */
          if (have > 0 && rc == SCR_SUCCESS) {
            /* compute crc of uncompressed block */
            crc_orig = crc32(crc_orig, (const Bytef*) buf_dst, (uInt) have);

            /* write uncompressed data to file */
            ssize_t nwrite = scr_write(file_dst, fd_dst, buf_dst, have);
            if (nwrite != have) {
              scr_err("Error writing to %s @ %s:%d",
                      file_dst, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }
          }
        } while ((ret == Z_OK || ret == Z_BUF_ERROR) && rc == SCR_SUCCESS); /* strm.avail_out == 0 */

        /* check that we uncompressed the entire block */
        if (strm.avail_in != 0 || ret != Z_STREAM_END) {
          scr_err("Failed to decompress file %s @ %s:%d",
                  file_src, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }

        /* add to total that we've read */
        total_read += nread;
      }
    }

    /* done with this block, check crc values */
    if (crc_cmp != file_crc_cmp) {
      scr_err("CRC32 mismatch detected in compressed block #%d in %s @ %s:%d",
              block_count, file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
    if (crc_orig != file_crc_orig) {
      scr_err("CRC32 mismatch detected in decompressed block #%d in %s @ %s:%d",
              block_count, file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* finalize the compression stream */
    inflateEnd(&strm);

    /* increment our block count */
    block_count++;
  }

  /* close files */
  scr_close(file_src, fd_src);
  scr_close(file_dst, fd_dst);

  /* TODO: truncate file */

  /* TODO: rename file */

  /* free our buffers */
  scr_align_free(&buf_dst);
  scr_align_free(&buf_src);
  scr_free(&header);

  return rc;
}
