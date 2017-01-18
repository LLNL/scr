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

#ifndef SCR_PARAM_H
#define SCR_PARAM_H

#include "scr_hash.h"

/* This will search a series of locations to find a given parameter name:
 *   environment variable
 *   user config file
 *   system config file
 *
 * It uses reference counting such that multiple callers may init and
 * finalize the parameters independently of one another.
*/

/* read config files and store contents */
int scr_param_init(void);

/* free contents from config files */
int scr_param_finalize(void);

/* searchs for name and returns a character pointer to its value if set,
 * returns NULL if not found */
char* scr_param_get(char* name);

/* searchs for name and returns a newly allocated hash of its value if set,
 * returns NULL if not found */
scr_hash* scr_param_get_hash(char* name);

#endif
