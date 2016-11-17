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

#ifndef SCR_FLUSH_ASYNC_H
#define SCR_FLUSH_ASYNC_H

#include "scr_filemap.h"

/* stop all ongoing asynchronous flush operations */
int scr_flush_async_stop(void);

/* start an asynchronous flush from cache to parallel file system under SCR_PREFIX */
int scr_flush_async_start(scr_filemap* map, int id);

/* check whether the flush from cache to parallel file system has completed */
int scr_flush_async_test(scr_filemap* map, int id, double* bytes);

/* complete the flush from cache to parallel file system */
int scr_flush_async_complete(scr_filemap* map, int id);

/* wait until the checkpoint currently being flushed completes */
int scr_flush_async_wait(scr_filemap* map);

/* shutdown the async transfer daemons */
int scr_flush_async_shutdown(void);
#endif
