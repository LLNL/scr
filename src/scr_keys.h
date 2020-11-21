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

#ifndef SCR_KEYS_H
#define SCR_KEYS_H

/*
=========================================
Define common hash key strings
========================================
*/

/* generic hash keys */
#define SCR_KEY_DATASET   ("DSET")
#define SCR_KEY_PATH      ("PATH")
#define SCR_KEY_SEGMENT   ("SEG")
#define SCR_KEY_CONTAINER ("CTR")
#define SCR_KEY_ID        ("ID")
#define SCR_KEY_NAME      ("NAME")
#define SCR_KEY_SIZE      ("SIZE")
#define SCR_KEY_OFFSET    ("OFFSET")
#define SCR_KEY_LENGTH    ("LENGTH")
#define SCR_KEY_RANK      ("RANK")
#define SCR_KEY_RANKS     ("RANKS")
#define SCR_KEY_DIRECTORY ("DIR")
#define SCR_KEY_FILE      ("FILE")
#define SCR_KEY_FILES     ("FILES")
#define SCR_KEY_META      ("META")
#define SCR_KEY_COMPLETE  ("COMPLETE")
#define SCR_KEY_CRC       ("CRC")

/* these keys are kept in hashes stored in files for long periods of time,
 * thus we associate a version number with them in order to read old files */
#define SCR_SUMMARY_KEY_VERSION ("VERSION")

#define SCR_SUMMARY_FILE_VERSION_5 (5)
#define SCR_SUMMARY_5_KEY_CKPT      ("CKPT")
#define SCR_SUMMARY_5_KEY_RANK      ("RANK")
#define SCR_SUMMARY_5_KEY_RANKS     ("RANKS")
#define SCR_SUMMARY_5_KEY_COMPLETE  ("COMPLETE")
#define SCR_SUMMARY_5_KEY_FILE      ("FILE")
#define SCR_SUMMARY_5_KEY_FILES     ("FILES")
#define SCR_SUMMARY_5_KEY_SIZE      ("SIZE")
#define SCR_SUMMARY_5_KEY_CRC       ("CRC")
#define SCR_SUMMARY_5_KEY_NOFETCH   ("NOFETCH")

#define SCR_SUMMARY_FILE_VERSION_6 (6)
#define SCR_SUMMARY_6_KEY_DATASET   ("DSET")
#define SCR_SUMMARY_6_KEY_RANK2FILE ("RANK2FILE")
#define SCR_SUMMARY_6_KEY_LEVEL     ("LEVEL")
#define SCR_SUMMARY_6_KEY_RANK      ("RANK")
#define SCR_SUMMARY_6_KEY_RANKS     ("RANKS")
#define SCR_SUMMARY_6_KEY_COMPLETE  ("COMPLETE")
#define SCR_SUMMARY_6_KEY_FILE      ("FILE")
#define SCR_SUMMARY_6_KEY_FILES     ("FILES")
#define SCR_SUMMARY_6_KEY_SIZE      ("SIZE")
#define SCR_SUMMARY_6_KEY_CRC       ("CRC")
#define SCR_SUMMARY_6_KEY_NOFETCH   ("NOFETCH")
#define SCR_SUMMARY_6_KEY_CONTAINER ("CTR")
#define SCR_SUMMARY_6_KEY_SEGMENT   ("SEG")
#define SCR_SUMMARY_6_KEY_ID        ("ID")
#define SCR_SUMMARY_6_KEY_LENGTH    ("LENGTH")
#define SCR_SUMMARY_6_KEY_OFFSET    ("OFFSET")

#define SCR_INDEX_KEY_VERSION ("VERSION")

#define SCR_INDEX_FILE_VERSION_1 (1)
#define SCR_INDEX_FILE_VERSION_2 (2)
#define SCR_INDEX_1_KEY_NAME      ("NAME")
#define SCR_INDEX_1_KEY_DIR       ("DIR")
#define SCR_INDEX_1_KEY_CKPT      ("CKPT")
#define SCR_INDEX_1_KEY_DATASET   ("DSET")
#define SCR_INDEX_1_KEY_COMPLETE  ("COMPLETE")
#define SCR_INDEX_1_KEY_FETCHED   ("FETCHED")
#define SCR_INDEX_1_KEY_FLUSHED   ("FLUSHED")
#define SCR_INDEX_1_KEY_FAILED    ("FAILED")
#define SCR_INDEX_1_KEY_CURRENT   ("CURRENT")

/* the rest of these hash keys are only used in memory or in files
 * that live for the life of the job, thus backwards compatibility is not needed */
#define SCR_FLUSH_KEY_DATASET  ("DATASET")
#define SCR_FLUSH_KEY_LOCATION ("LOCATION")
#define SCR_FLUSH_KEY_LOCATION_CACHE    ("CACHE")
#define SCR_FLUSH_KEY_LOCATION_PFS      ("PFS")
#define SCR_FLUSH_KEY_LOCATION_FLUSHING ("FLUSHING")
#define SCR_FLUSH_KEY_LOCATION_SYNC_FLUSHING ("SYNC_FLUSHING")
#define SCR_FLUSH_KEY_DIRECTORY ("DIR")
#define SCR_FLUSH_KEY_NAME      ("NAME")
#define SCR_FLUSH_KEY_CKPT      ("CKPT")
#define SCR_FLUSH_KEY_OUTPUT    ("OUTPUT")
#define SCR_FLUSH_KEY_DSETDESC  ("DSETDESC")

#define SCR_SCAVENGE_KEY_PARTNER   ("PARTNER")

#define SCR_NODES_KEY_NODES ("NODES")

/* transfer file keys */
#define SCR_TRANSFER_KEY_FILES       ("FILES")
#define SCR_TRANSFER_KEY_DESTINATION ("DESTINATION")
#define SCR_TRANSFER_KEY_SIZE        ("SIZE")
#define SCR_TRANSFER_KEY_WRITTEN     ("WRITTEN")
#define SCR_TRANSFER_KEY_BW          ("BW")
#define SCR_TRANSFER_KEY_PERCENT     ("PERCENT")

#define SCR_TRANSFER_KEY_COMMAND ("COMMAND")
#define SCR_TRANSFER_KEY_COMMAND_RUN  ("RUN")
#define SCR_TRANSFER_KEY_COMMAND_STOP ("STOP")
#define SCR_TRANSFER_KEY_COMMAND_EXIT ("EXIT")

#define SCR_TRANSFER_KEY_STATE ("STATE")
#define SCR_TRANSFER_KEY_STATE_RUN  ("RUNNING")
#define SCR_TRANSFER_KEY_STATE_STOP ("STOPPED")
#define SCR_TRANSFER_KEY_STATE_EXIT ("EXITING")

#define SCR_TRANSFER_KEY_FLAG ("FLAG")
#define SCR_TRANSFER_KEY_FLAG_DONE ("DONE")

/* ckpt config file keys */
#define SCR_CONFIG_KEY_GROUPDESC  ("GROUPS")
#define SCR_CONFIG_KEY_STOREDESC  ("STORE")
#define SCR_CONFIG_KEY_CACHEDESC  ("CACHE")
#define SCR_CONFIG_KEY_COUNT      ("COUNT")
#define SCR_CONFIG_KEY_NAME       ("NAME")
#define SCR_CONFIG_KEY_BASE       ("BASE")
#define SCR_CONFIG_KEY_STORE      ("STORE")
#define SCR_CONFIG_KEY_SIZE       ("SIZE")
#define SCR_CONFIG_KEY_GROUP      ("GROUP")

#define SCR_CONFIG_KEY_CKPTDESC   ("CKPT")
#define SCR_CONFIG_KEY_ENABLED    ("ENABLED")
#define SCR_CONFIG_KEY_INDEX      ("INDEX")
#define SCR_CONFIG_KEY_INTERVAL   ("INTERVAL")
#define SCR_CONFIG_KEY_OUTPUT     ("OUTPUT")
#define SCR_CONFIG_KEY_BYPASS     ("BYPASS")
#define SCR_CONFIG_KEY_DIRECTORY  ("DIRECTORY")
#define SCR_CONFIG_KEY_TYPE       ("TYPE")
#define SCR_CONFIG_KEY_FAIL_GROUP ("FAIL_GROUP")
#define SCR_CONFIG_KEY_SET_SIZE     ("SET_SIZE")
#define SCR_CONFIG_KEY_SET_FAILURES ("SET_FAILURES")
#define SCR_CONFIG_KEY_GROUPS     ("GROUPS")
#define SCR_CONFIG_KEY_GROUP_ID   ("GROUP_ID")
#define SCR_CONFIG_KEY_GROUP_SIZE ("GROUP_SIZE")
#define SCR_CONFIG_KEY_GROUP_RANK ("GROUP_RANK")
#define SCR_CONFIG_KEY_MKDIR      ("MKDIR")
#define SCR_CONFIG_KEY_FLUSH      ("FLUSH")
#define SCR_CONFIG_KEY_VIEW       ("VIEW")

#define SCR_META_KEY_CKPT     ("CKPT")
#define SCR_META_KEY_RANKS    ("RANKS")
#define SCR_META_KEY_RANK     ("RANK")
#define SCR_META_KEY_ORIG     ("ORIG")
#define SCR_META_KEY_PATH     ("PATH")
#define SCR_META_KEY_NAME     ("NAME")
#define SCR_META_KEY_SIZE     ("SIZE")
#define SCR_META_KEY_CRC      ("CRC")
#define SCR_META_KEY_COMPLETE ("COMPLETE")
#define SCR_META_KEY_MODE     ("MODE")
#define SCR_META_KEY_UID      ("UID")
#define SCR_META_KEY_GID      ("GID")
#define SCR_META_KEY_ATIME_SECS  ("ATIME_SECS")
#define SCR_META_KEY_ATIME_NSECS ("ATIME_NSECS")
#define SCR_META_KEY_CTIME_SECS  ("CTIME_SECS")
#define SCR_META_KEY_CTIME_NSECS ("CTIME_NSECS")
#define SCR_META_KEY_MTIME_SECS  ("MTIME_SECS")
#define SCR_META_KEY_MTIME_NSECS ("MTIME_NSECS")

#define SCR_KEY_COPY_XOR_CHUNK   ("CHUNK")
#define SCR_KEY_COPY_XOR_DATASET ("DSET")
#define SCR_KEY_COPY_XOR_CURRENT ("CURRENT")
#define SCR_KEY_COPY_XOR_PARTNER ("PARTNER")
#define SCR_KEY_COPY_XOR_FILES   ("FILES")
#define SCR_KEY_COPY_XOR_FILE    ("FILE")
#define SCR_KEY_COPY_XOR_RANKS   ("RANKS")
#define SCR_KEY_COPY_XOR_RANK    ("RANK")
#define SCR_KEY_COPY_XOR_GROUP   ("GROUP")
#define SCR_KEY_COPY_XOR_GROUP_RANKS ("RANKS")
#define SCR_KEY_COPY_XOR_GROUP_RANK  ("RANK")

#endif
