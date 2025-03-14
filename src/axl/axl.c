#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>

/* PATH_MAX */
#include <limits.h>

/* dirname */
#include <libgen.h>

/* mkdir */
#include <sys/types.h>
#include <sys/stat.h>

/* opendir */
#include <dirent.h>

/* axl_xfer_t */
#include "axl.h"

/* kvtree & everything else */
#include "axl_internal.h"
#include "kvtree.h"
#include "kvtree_util.h"

#include "config.h"

/* xfer methods */
#include "axl_sync.h"

#ifdef HAVE_PTHREADS
#include "axl_pthread.h"
#endif /* HAVE_PTHREAD */

#ifdef HAVE_BBAPI
#include "axl_async_bbapi.h"
#endif /* HAVE_BBAPI */

#ifdef HAVE_DATAWARP
#include "axl_async_datawarp.h"
#endif /* HAVE_DATAWARP */

/* define states for transfer handlesto help ensure
 * users call AXL functions in the correct order */
typedef enum {
    AXL_XFER_STATE_NULL,       /* placeholder for invalid state */
    AXL_XFER_STATE_CREATED,    /* handle has been created */
    AXL_XFER_STATE_DISPATCHED, /* transfer has been dispatched */
    AXL_XFER_STATE_WAITING,    /* wait has been called */
    AXL_XFER_STATE_COMPLETED,  /* files are all copied */
    AXL_XFER_STATE_CANCELED,   /* transfer was AXL_Cancel'd */
} axl_xfer_state_t;

/* Temporary extension added onto files while they're being transferred. */
#define AXL_EXTENSION "._AXL"

/*
=========================================
Global Variables
========================================
*/

/* whether axl should first create parent directories
 * before transferring files */
int axl_make_directories;

/* whether to first copy files to temporary name with an extension */
int axl_use_extension;

/* track whether file metadata should also be copied */
int axl_copy_metadata;

/* global rank of calling process, used for BBAPI */
int axl_rank = -1;

/* reference count for number of times AXL_Init has been called */
static unsigned int axl_init_count = 0;

/* Array for all the AXL_Create'd kvtree pointers.  It's indexed by the AXL id.
 *
 * Note: We only expand this array, we never shrink it.  This is fine since
 * the user is only going to be calling AXL_Create() a handful of times.  It
 * also simplifies the code if we never shrink it, and the extra memory usage
 * is negligible, if any at all. */
kvtree** axl_kvtrees;
static unsigned int axl_kvtrees_count = 0;

#ifdef HAVE_BBAPI
static int bbapi_is_loaded = 0;
#endif

/* Allocate a new kvtree and return the AXL ID for it.  If state_file is
 * specified, then populate the kvtree with it's data. */
static int axl_alloc_id(const char* state_file)
{
    kvtree* new = kvtree_new();

    /* initialize kvtree values from state_file if we have one */
    if (state_file) {
        if (access(state_file, F_OK) == 0 &&
            kvtree_read_file(state_file, new) != KVTREE_SUCCESS)
        {
            AXL_ERR("Couldn't read state file correctly");
            return -1;
        }

        /* record name of state file */
        kvtree_util_set_str(new, AXL_KEY_STATE_FILE, state_file);
    }

    int id = axl_kvtrees_count;
    axl_kvtrees_count++;

    axl_kvtrees = realloc(axl_kvtrees, sizeof(struct kvtree*) * axl_kvtrees_count);
    axl_kvtrees[id] = new;

    return id;
}

/* Remove the state file for an id, if one exists */
static void axl_remove_state_file(int id)
{
    kvtree* file_list = axl_kvtrees[id];
    char* state_file = NULL;
    if (kvtree_util_get_str(file_list, AXL_KEY_STATE_FILE,
        &state_file) == KVTREE_SUCCESS)
    {
        axl_file_unlink(state_file);
    }
}

/* Remove the ID, we don't need it anymore */
static void axl_free_id(int id)
{
    axl_remove_state_file(id);

    /* kvtree_delete() will set axl_kvtrees[id] = NULL */
    kvtree_delete(&axl_kvtrees[id]);
}

/* If the user specified a state_file then write our kvtree to it. If not, then
 * do nothing. */
void axl_write_state_file(int id)
{
    kvtree* file_list = axl_kvtrees[id];
    char* state_file = NULL;
    if (kvtree_util_get_str(file_list, AXL_KEY_STATE_FILE,
        &state_file) == KVTREE_SUCCESS)
    {
        kvtree_write_file(state_file, file_list);
    }
}

/* given an id, lookup and return the file list and transfer type,
 * returns AXL_FAILURE if info could not be found */
static int axl_get_info(int id, kvtree** list, axl_xfer_t* type, axl_xfer_state_t* state)
{
    /* initialize output parameters to invalid values */
    *list  = NULL;
    *type  = AXL_XFER_NULL;
    *state = AXL_XFER_STATE_NULL;

    /* lookup transfer info for the given id */
    kvtree* file_list = axl_kvtrees[id];
    if (file_list == NULL) {
        AXL_ERR("Could not find fileset for UID %d", id);
        return AXL_FAILURE;
    }

    /* extract the transfer type */
    int itype;
    if (kvtree_util_get_int(file_list, AXL_KEY_XFER_TYPE, &itype) != KVTREE_SUCCESS) {
        AXL_ERR("Could not find transfer type for UID %d", id);
        return AXL_FAILURE;
    }
    axl_xfer_t xtype = (axl_xfer_t) itype;

    /* extract the transfer state */
    int istate;
    if (kvtree_util_get_int(file_list, AXL_KEY_STATE, &istate) != KVTREE_SUCCESS) {
        AXL_ERR("Could not find transfer state for UID %d", id);
        return AXL_FAILURE;
    }
    axl_xfer_state_t xstate = (axl_xfer_state_t) istate;

    /* set output parameters */
    *list  = file_list;
    *type  = xtype;
    *state = xstate;

    return AXL_SUCCESS;
}

/* Return the native underlying transfer API for this particular node.  If you're
 * running on an IBM node, use the BB API.  If you're running on a Cray, use
 * DataWarp.  Otherwise use sync. */
static axl_xfer_t axl_detect_native_xfer(void)
{
    axl_xfer_t xtype = AXL_XFER_NULL;

    /* In an ideal world, we would detect our node type at runtime, since
     * *technically* we could be compiled with support for both the BB API and
     * DataWarp libraries.  In the real world, our supercomputer is only going
     * to have one of those libraries, so just use whatever we find at
     * build time. */
#ifdef HAVE_BBAPI
    xtype = AXL_XFER_ASYNC_BBAPI;
#elif HAVE_DATAWARP
    xtype = AXL_XFER_ASYNC_DW;
#else
    xtype = AXL_XFER_SYNC;
#endif

    return xtype;
}

/* Return the fastest API that's also compatible with all AXL transfers.  We
 * need this since there may be some edge case transfers the native APIs don't
 * support. */
static axl_xfer_t axl_detect_default_xfer(void)
{
    axl_xfer_t xtype = axl_detect_native_xfer();

    /* BBAPI doesn't support shmem, so we can't use it by default. */
    if (xtype == AXL_XFER_ASYNC_BBAPI) {
        xtype = AXL_XFER_SYNC;
    }

    return xtype;
}

/*
=========================================
API Functions
========================================
*/

/* Initialize library and start up vendor specific services */
int AXL_Init (void)
{
    int rc = AXL_SUCCESS;

    /* TODO: set these by config file */
    axl_file_buf_size = (unsigned long) (32UL * 1024UL * 1024UL);

    /* initialize our debug level for filtering AXL_DBG messages */
    axl_debug = 0;
    char* val = getenv("AXL_DEBUG");
    if (val != NULL) {
        axl_debug = atoi(val);
    }

    /* whether axl should first create parent directories
     * before transferring files */
    axl_make_directories = 1;
    val = getenv("AXL_MKDIR");
    if (val != NULL) {
        axl_make_directories = atoi(val);
    }

    /* initialize our flag on whether to first copy files to temporary names with extension */
    axl_use_extension = 0;
    val = getenv("AXL_USE_EXTENSION");
    if (val != NULL) {
        axl_use_extension = atoi(val);
    }

    /* initialize our flag on whether to copy file metadata */
    axl_copy_metadata = 0;
    val = getenv("AXL_COPY_METADATA");
    if (val != NULL) {
        axl_copy_metadata = atoi(val);
    }

    /* keep a reference count to free memory on last AXL_Finalize */
    axl_init_count++;

    return rc;
}

/* Shutdown any vendor services */
int AXL_Finalize (void)
{
    int rc = AXL_SUCCESS;

#ifdef HAVE_BBAPI
    if (bbapi_is_loaded) {
       if (axl_async_finalize_bbapi() != AXL_SUCCESS) {
          rc = AXL_FAILURE;
       }
    }
#endif

    /* decrement reference count and free data structures on last call */
    axl_init_count--;
    if (axl_init_count == 0) {
        /* TODO: are there cases where we also need to delete trees? */
        axl_free(&axl_kvtrees);
        axl_kvtrees_count = 0;
    }

    return rc;
}

/** Actual function to set config parameters */
static kvtree* AXL_Config_Set(const kvtree* config)
{
    assert(config != NULL);

    kvtree* retval = (kvtree*)(config);

    static const char* known_global_options[] = {
        AXL_KEY_CONFIG_FILE_BUF_SIZE,
        AXL_KEY_CONFIG_DEBUG,
        AXL_KEY_CONFIG_MKDIR,
        AXL_KEY_CONFIG_USE_EXTENSION,
        AXL_KEY_CONFIG_COPY_METADATA,
        AXL_KEY_CONFIG_RANK,
        NULL
    };
    static const char* known_transfer_options[] = {
        AXL_KEY_CONFIG_FILE_BUF_SIZE,
        AXL_KEY_CONFIG_MKDIR,
        AXL_KEY_CONFIG_USE_EXTENSION,
        AXL_KEY_CONFIG_COPY_METADATA,
        NULL
    };

    /* global options */
    /* TODO: this could be turned into a list of structs */
    kvtree_util_get_bytecount(config,
        AXL_KEY_CONFIG_FILE_BUF_SIZE, &axl_file_buf_size);

    kvtree_util_get_int(config,
        AXL_KEY_CONFIG_DEBUG, &axl_debug);

    kvtree_util_get_int(config,
        AXL_KEY_CONFIG_MKDIR, &axl_make_directories);

    kvtree_util_get_int(config,
        AXL_KEY_CONFIG_USE_EXTENSION, &axl_use_extension);

    kvtree_util_get_int(config,
        AXL_KEY_CONFIG_COPY_METADATA, &axl_copy_metadata);

    kvtree_util_get_int(config,
        AXL_KEY_CONFIG_RANK, &axl_rank);

    /* check for local options inside an "id" subkey */
    kvtree* ids = kvtree_get(config, "id");
    if (ids != NULL) {
        const kvtree_elem* elem;
        for (elem = kvtree_elem_first(ids); elem != NULL && retval != NULL;
             elem = kvtree_elem_next(elem))
        {
            const char* key = kvtree_elem_key(elem);

            char* endptr;
            long id = strtol(key, &endptr, 10);
            if ((*key == '\0' || *endptr != '\0') ||
                (id < 0 || id >= axl_kvtrees_count))
            {
                retval = NULL;
                break;
            }

            kvtree* local_config = kvtree_elem_hash(elem);
            if (local_config == NULL) {
                retval = NULL;
                break;
            }

            kvtree* file_list = axl_kvtrees[id];

            const char** opt;
            for (opt = known_transfer_options; *opt != NULL; opt++) {
                const char* val = kvtree_get_val(local_config, *opt);
                if (val != NULL) {
                    /* this is (annoyingly) non-atomic so could leave file_list in
                     * a strange state if malloc() fails at the wrong time.
                     * Using kvtree_merge with a temporary tree does not seem to be any
                     * better though */
                    if (kvtree_util_set_str(file_list, *opt, val) != KVTREE_SUCCESS) {
                        retval = NULL;
                        break;
                    }
                }
            }

            /* report all unknown options (typos?) */
            /* TODO: move into a function, is used twice (well almost, differs in
             * whether "id" is acceptable */
            const kvtree_elem* local_elem;
            for (local_elem = kvtree_elem_first(local_config);
                 local_elem != NULL;
                 local_elem = kvtree_elem_next(local_elem))
            {
                /* must be only one level deep, ie plain kev = value */
                const kvtree* elem_hash = kvtree_elem_hash(local_elem);
                assert(kvtree_size(elem_hash) == 1);

                const kvtree* kvtree_first_elem_hash =
                  kvtree_elem_hash(kvtree_elem_first(elem_hash));
                assert(kvtree_size(kvtree_first_elem_hash) == 0);

                /* check against known options */
                int found = 0;
                const char** opt;
                for (opt = known_transfer_options; *opt != NULL; opt++) {
                    if (strcmp(*opt, kvtree_elem_key(local_elem)) == 0) {
                        found = 1;
                        break;
                    }
                }
                if (! found) {
                    AXL_ERR("Unknown configuration parameter '%s' with value '%s'",
                      kvtree_elem_key(local_elem),
                      kvtree_elem_key(kvtree_elem_first(kvtree_elem_hash(local_elem)))
                    );
                    retval = NULL;
                    break;
                }
            }
        }
    }

    /* report all unknown options (typos?) */
    const kvtree_elem* elem;
    for (elem = kvtree_elem_first(config);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        const char* key = kvtree_elem_key(elem);
        if (strcmp("id", key) == 0) {
            continue;
        }

        /* must be only one level deep, ie plain kev = value */
        const kvtree* elem_hash = kvtree_elem_hash(elem);
        assert(kvtree_size(elem_hash) == 1);

        const kvtree* kvtree_first_elem_hash =
          kvtree_elem_hash(kvtree_elem_first(elem_hash));
        assert(kvtree_size(kvtree_first_elem_hash) == 0);

        /* check against known options */
        int found = 0;
        const char** opt;
        for (opt = known_global_options; *opt != NULL; opt++) {
            if (strcmp(*opt, key) == 0) {
                found = 1;
                break;
            }
        }
        if (! found) {
            AXL_ERR("Unknown configuration parameter '%s' with value '%s'",
              key, kvtree_elem_key(kvtree_elem_first(kvtree_elem_hash(elem)))
            );
            retval = NULL;
        }
    }

    return retval;
}

/** Actual function to get config parameters */
static kvtree* AXL_Config_Get()
{
    kvtree* config = kvtree_new();
    assert(config != NULL);

    static const char* known_options[] = {
        AXL_KEY_CONFIG_FILE_BUF_SIZE,
        AXL_KEY_CONFIG_MKDIR,
        AXL_KEY_CONFIG_USE_EXTENSION,
        AXL_KEY_CONFIG_COPY_METADATA,
        NULL
    };

    int success = 1; /* all values could be set? */

    /* global options */
    success &= kvtree_util_set_bytecount(config,
        AXL_KEY_CONFIG_FILE_BUF_SIZE, axl_file_buf_size) == KVTREE_SUCCESS;

    success &= kvtree_util_set_int(config,
        AXL_KEY_CONFIG_DEBUG, axl_debug) == KVTREE_SUCCESS;

    success &= kvtree_util_set_int(config,
        AXL_KEY_CONFIG_MKDIR, axl_make_directories) == KVTREE_SUCCESS;

    success &= kvtree_util_set_int(config,
        AXL_KEY_CONFIG_USE_EXTENSION, axl_use_extension) == KVTREE_SUCCESS;

    success &= kvtree_util_set_int(config,
        AXL_KEY_CONFIG_COPY_METADATA, axl_copy_metadata) == KVTREE_SUCCESS;

    success &= kvtree_util_set_int(config,
        AXL_KEY_CONFIG_RANK, axl_rank) == KVTREE_SUCCESS;

    /* per transfer options */
    int id;
    for (id = 0; id < axl_kvtrees_count; id++) {
        kvtree* file_list = axl_kvtrees[id];
        if (file_list == NULL) {
            /* TODO: check if it would be better to return an empty hash instead */
            continue;
        }

        kvtree* new = kvtree_set_kv_int(config, "id", id);
        if (new == NULL) {
            success = 0;
            break;
        }
        /* get all known options */
        const char** opt;
        for (opt = known_options; *opt != NULL; opt++) {
            const char* val = kvtree_get_val(file_list, *opt);
            if (val != NULL) {
                /* this is (annoyingly) non-atomic so could leave new in
                 * a strange state if malloc() fails at the wrong time.
                 * Using kvtree_merge with a temporary tree does not seem to be any
                 * better though */
                if (kvtree_util_set_str(new, *opt, val) != KVTREE_SUCCESS) {
                    success = 0;
                    break;
                }
            } else {
                /* these options must exist, if not something is wrong */
                success = 0;
                break;
            }
        }
    }
    if (!success) {
        kvtree_delete(&config);
    }

    return config;
}

/** Set a AXL config parameters */
kvtree* AXL_Config(const kvtree* config)
{
    if (config != NULL) {
        return AXL_Config_Set(config);
    } else {
        return AXL_Config_Get();
    }
}

/* Create a transfer handle (used for 0+ files)
 * Type specifies a particular method to use
 * Name is a user/application provided string
 * Returns an ID to the transfer handle */
int AXL_Create(axl_xfer_t xtype, const char* name, const char* state_file)
{
    /* Generate next unique ID */
    int id = axl_alloc_id(state_file);
    if (id < 0) {
        return id;
    }

    /*
     * If no state file is passed, or it doesn't exist yet, then we're starting
     * from scratch.
     */
    int reload_from_state_file = 1;
    if (!state_file || (state_file && access(state_file, F_OK) != 0)) {
        reload_from_state_file = 0;
    }

    /* Create an entry for this transfer handle
     * record user string and transfer type
     * UID
     *   id
     *     NAME
     *       name
     *     TYPE
     *       type_enum
     *     STATUS
     *       SOURCE
     *     STATE
     *       CREATED */
    if (reload_from_state_file) {
        kvtree* file_list = NULL;
        axl_xfer_t old_xtype = AXL_XFER_NULL;
        axl_xfer_state_t old_xstate = AXL_XFER_STATE_NULL;
        if (axl_get_info(id, &file_list, &old_xtype, &old_xstate) != AXL_SUCCESS) {
            AXL_ERR("Couldn't get kvtree info");
            return -1;
        }

        if (xtype == AXL_XFER_STATE_FILE) {
            xtype = old_xtype;
        }

        if (xtype != old_xtype) {
            AXL_ERR("Transfer type %d doesn't match transfer type %d "
                "in state_file", xtype, old_xtype);
            return -1;
        }
    }

    if (xtype == AXL_XFER_DEFAULT) {
        xtype = axl_detect_default_xfer();
    } else if (xtype == AXL_XFER_NATIVE) {
        xtype = axl_detect_native_xfer();
    } else if (xtype == AXL_XFER_STATE_FILE && !reload_from_state_file) {
        AXL_ERR("Can't use AXL_XFER_STATE_FILE without a state_file");
        return -1;
    }

    kvtree* file_list = axl_kvtrees[id];
    kvtree_util_set_int(file_list, AXL_KEY_XFER_TYPE, xtype);
    kvtree_util_set_str(file_list, AXL_KEY_UNAME, name);
    if (!reload_from_state_file) {
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_SOURCE);
        kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_CREATED);

        /* options */
        /* TODO: handle return code of kvtree_util_set_XXX */
        /* TODO: handle differnces between size_t and unsigned long */
        kvtree_util_set_bytecount(file_list,
            AXL_KEY_CONFIG_FILE_BUF_SIZE, axl_file_buf_size);

        /* a per transfer debug value is not currently supported
        success &= kvtree_util_set_int(file_list,
            AXL_KEY_CONFIG_DEBUG, axl_debug) == KVTREE_SUCCESS;
        */

        kvtree_util_set_int(file_list,
            AXL_KEY_CONFIG_MKDIR, axl_make_directories);

        kvtree_util_set_int(file_list,
            AXL_KEY_CONFIG_USE_EXTENSION, axl_use_extension);

        kvtree_util_set_int(file_list,
            AXL_KEY_CONFIG_COPY_METADATA, axl_copy_metadata);
    }

    /* create a structure based on transfer type */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        break;

    case AXL_XFER_PTHREAD:
#ifdef HAVE_PTHREADS
        break;
#else
        /* User is requesting a pthreads transfer, but we didn't build with pthreads */
        AXL_ERR("pthreads requested but not enabled during build");
        rc = AXL_FAILURE;
        break;
#endif /* HAVE_PTHREADS */

    case AXL_XFER_ASYNC_BBAPI:
#ifdef HAVE_BBAPI
        /* Load the BB library on the very first call to
         * AXL_Create(AXL_XFER_BBAPI, ...).  We have to do it here instead
         * of in AXL_Init(), since this is the first point that we know
         * we're doing an actual BB API transfer. */
        if (!bbapi_is_loaded) {
           rc = axl_async_init_bbapi();
           if (rc != AXL_SUCCESS) {
              break;
           }
           bbapi_is_loaded = 1;
        }
        if (! reload_from_state_file) {
            rc = axl_async_create_bbapi(id);
        }
#else
        /* User is requesting a BB transfer, but we didn't build with BBAPI */
        AXL_ERR("BBAPI requested but not enabled during build");
        rc = AXL_FAILURE;
#endif /* HAVE_BBAPI */
        break;

    case AXL_XFER_ASYNC_DW:
#ifndef HAVE_DATAWARP
        /* User is requesting a datawarp transfer, but we didn't build with datawarp */
        AXL_ERR("Datawarp requested but not enabled during build");
        rc = AXL_FAILURE;
#endif /* HAVE_DATAWARP */
        break;

    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* clear entry from our list if something went wrong */
    if (rc != AXL_SUCCESS) {
        axl_free_id(id);
        id = -1;
    } else {
        /* Success, write data to file if we have one */
        axl_write_state_file(id);
    }

    return id;
}

/* Is this path a file or a directory?  Return the type. */
enum {PATH_UNKNOWN = 0, PATH_FILE, PATH_DIR};

static int path_type(const char *path)
{
    struct stat s;
    if (stat(path, &s) != 0) {
        return PATH_UNKNOWN;
    }
    if (S_ISREG(s.st_mode)) {
        return PATH_FILE;
    }
    if (S_ISDIR(s.st_mode)) {
        return PATH_DIR;
    }
    return PATH_UNKNOWN;
}

/* Given a path name to a file ('path'), add on an extra AXL temporary extension
 * in the form of: path._AXL[extra].  So if path = '/tmp/file1' and
 * extra = "1234", then the new name is /tmp/file1._AXL1234.  The new name is
 * allocated and returned as a new string (which must be freed).
 *
 * 'extra' can be optionally used to store additional information about the
 * transfer, such as the BB API transfer handle number, or it can be left NULL. */
static char* axl_add_extension(const char* path, const char* extra)
{
    char* tmp = NULL;
    asprintf(&tmp, "%s%s%s", path, AXL_EXTENSION, extra ? extra : "");
    return tmp;
}

/* Add a file to an existing transfer handle.  No directories.
 *
 * If the file's destination path doesn't exist, then automatically create the
 * needed directories. */
static int __AXL_Add (int id, const char* src, const char* dest)
{
    int rc = AXL_SUCCESS;

    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to add files */
    if (xstate != AXL_XFER_STATE_CREATED) {
        AXL_ERR("Invalid state to add files for UID %d", id);
        return AXL_FAILURE;
    }

    /* add record for this file
     * UID
     *   id
     *     FILES
     *       /path/to/src/file
     *         DEST
     *           /path/to/dest/file
     *         STATUS
     *           SOURCE */
    kvtree* src_hash = kvtree_set_kv(file_list, AXL_KEY_FILES, src);

    if (axl_use_extension) {
        char* extra = NULL;

#ifdef HAVE_BBAPI
        /* Special case: For BB API we includes the transfer handle in the temporary
         * file extension.  That way, we can later use it to lookup the transfer
         * handle for old transfers and cancel them. */
        if (xtype == AXL_XFER_ASYNC_BBAPI) {
            uint64_t thandle;
            if (axl_async_get_bbapi_handle(id, &thandle) !=0 ) {
                AXL_ERR("Couldn't get BB API transfer handle");
                return AXL_FAILURE;
            }

            asprintf(&extra, "%lu", thandle);
        }
#endif

        char* newdest = axl_add_extension(dest, extra);
        kvtree_util_set_str(src_hash, AXL_KEY_FILE_DEST, newdest);
        axl_free(&newdest);

        axl_free(&extra);
    } else {
        kvtree_util_set_str(src_hash, AXL_KEY_FILE_DEST, dest);
    }

    kvtree_util_set_int(src_hash, AXL_KEY_STATUS, AXL_STATUS_SOURCE);

    /* add file to transfer data structure, depending on its type */
    switch (xtype) {
    case AXL_XFER_SYNC:
        break;

#ifdef HAVE_PTHREADS
    case AXL_XFER_PTHREAD:
        break;
#endif /* HAVE_PTHREADS */

#ifdef HAVE_BBAPI
    case AXL_XFER_ASYNC_BBAPI:
        /* Special case:
         * The BB API checks to see if the destination path exists at
         * BB_AddFiles() time (analogous to AXL_Add()).  This is an issue
         * since the destination paths get mkdir'd in AXL_Dispatch()
         * and thus aren't available yet.  That's why we hold off on
         * doing our BB_AddFiles() calls until AXL_Dispatch(). */
        break;
#endif /* HAVE_BBAPI */

#ifdef HAVE_DATAWARP
    case AXL_XFER_ASYNC_DW:
        break;
#endif /* HAVE_DATAWARP */

    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* write data to file if we have one */
    axl_write_state_file(id);

    return rc;
}

/* Add a file or directory to the transfer handle.  If the src is a
 * directory, recursively add all the files and directories in that
 * directory. */
int AXL_Add (int id, const char* src, const char* dest)
{
    int rc = AXL_SUCCESS;

    char* new_dest = calloc(PATH_MAX, 1);
    if (! new_dest) {
        return AXL_FAILURE;
    }

    char* new_src = calloc(PATH_MAX, 1);
    if (! new_src) {
        free(new_dest);
        return AXL_FAILURE;
    }

    char* final_dest = calloc(PATH_MAX, 1);
    if (! final_dest) {
        free(new_dest);
        free(new_src);
        return AXL_FAILURE;
    }

    char* src_copy  = strdup(src);
    char* dest_copy = strdup(dest);

    unsigned int src_path_type  = path_type(src);
    unsigned int dest_path_type = path_type(dest);

    char* src_basename = basename(src_copy);

    DIR *dir;
    struct dirent *de;
    switch (src_path_type) {
    case PATH_FILE:
        if (dest_path_type == PATH_DIR) {
            /* They passed a source file, with dest directory.  Append the
             * filename to dest.
             *
             * Before:
             * src          dest
             * /tmp/file1   /tmp/mydir
             *
             * After:
             * /tmp/file1   /tmp/mydir/file1 */

            snprintf(new_dest, PATH_MAX, "%s/%s", dest, src_basename);
            rc = __AXL_Add(id, src, new_dest);
        } else {
            /* The destination is a filename */
            rc = __AXL_Add(id, src, dest);
        }
        break;
    case PATH_DIR:
        /* Add the directory itself first... */
        if (dest_path_type == PATH_FILE) {
            /* We can't copy a directory onto a file */
            rc = AXL_FAILURE;
            break;
       } else if (dest_path_type == PATH_DIR) {
            snprintf(new_dest, PATH_MAX, "%s/%s", dest, src_basename);
        } else {
            /* Our destination doesn't exist */
            snprintf(new_dest, PATH_MAX, "%s", dest);
        }

        /* Traverse all files/dirs in the directory. */
        dir = opendir(src);
        if (! dir) {
            rc = AXL_FAILURE;
            break;
        }

        while ((de = readdir(dir)) != NULL) {
            /* Skip '.' and '..' directories */
            if ((strcmp(de->d_name, ".") == 0) || (strcmp(de->d_name, "..") == 0)) {
                continue;
            }

            snprintf(new_src, PATH_MAX, "%s/%s", src, de->d_name);
            snprintf(final_dest, PATH_MAX, "%s/%s", new_dest, de->d_name);

            rc = AXL_Add(id, new_src, final_dest);
            if (rc != AXL_SUCCESS) {
                rc = AXL_FAILURE;
                break;
            }
        }
        break;

    default:
        rc = AXL_FAILURE;
        break;
    }

    free(dest_copy);
    free(src_copy);
    free(final_dest);
    free(new_src);
    free(new_dest);

    return rc;
}

/* Save metadata (size & mode bits) about each file to the file_list kvtree.
 *
 * TODO: Make this multithreaded. */
static int axl_save_metadata(int id)
{
    /* For each source file ... */
    char* src;
    kvtree_elem* elem = NULL;
    while ((elem = axl_get_next_path(id, elem, &src, NULL))) {
        /* Get the kvtree for the file */
        kvtree* src_kvtree = kvtree_elem_hash(elem);

        /* stat() the file and record metadata to the file's kvtree */
        int rc = axl_meta_encode(src, src_kvtree);
        if (rc != AXL_SUCCESS) {
            return rc;
        }
    }
    return AXL_SUCCESS;
}

/* Given an AXL id, check that all the file sizes are correct after a
 * transfer.
 *
 * TODO: Make this multithreaded */
static int axl_check_file_sizes(int id)
{
    /* For each source file ... */
    char* dst;
    kvtree_elem* elem = NULL;
    while ((elem = axl_get_next_path(id, elem, NULL, &dst))) {
        /* Get the kvtree for the file */
        kvtree* src_kvtree = kvtree_elem_hash(elem);

        int rc = axl_check_file_size(dst, src_kvtree);
        if (rc != AXL_SUCCESS) {
            return rc;
        }
    }
    return AXL_SUCCESS;
}

/* Set metadata mode bits
 *
 * TODO: Make this multithreaded. */
static int axl_set_metadata(int id)
{
    /* For each destination file ... */
    char* src;
    char* dst;
    kvtree_elem* elem = NULL;
    while ((elem = axl_get_next_path(id, elem, &src, &dst))) {
        kvtree* src_kvtree = kvtree_elem_hash(elem);

        int rc = axl_meta_apply(dst, src_kvtree);
        if (rc != AXL_SUCCESS) {
            return rc;
        }
    }
    return AXL_SUCCESS;
}

/* Initiate a transfer for all files in handle ID.  If resume is set to 1, then
 * attempt to resume the transfers from the existing destination files.
 *
 * If resume is set, but some of the destination files don't exist, then do
 * a normal transfer for them. */
int __AXL_Dispatch (int id, int resume)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    if (resume) {
        switch (xstate) {
        case AXL_XFER_STATE_NULL:
            AXL_ERR("Starting AXL_Resume() in state AXL_XFER_STATE_NULL");
            return AXL_FAILURE;
            break;
        case AXL_XFER_STATE_COMPLETED:
            /* We're trying to resume a transfer that's already completed, so
             * we're done! */
            return AXL_SUCCESS;
            break;
        case AXL_XFER_STATE_CREATED:
        case AXL_XFER_STATE_DISPATCHED:
        case AXL_XFER_STATE_WAITING:
        case AXL_XFER_STATE_CANCELED:
            /* Start or resume the transfer */
            break;
        }
    } else if (xstate != AXL_XFER_STATE_CREATED) {
        /* check that handle is in correct state to dispatch */
        AXL_ERR("Invalid state to dispatch UID %d", id);
        return AXL_FAILURE;
    }

    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_DISPATCHED);

    int make_directories;
    int success = kvtree_util_get_int(file_list,
        AXL_KEY_CONFIG_MKDIR, &make_directories);
    assert(success == KVTREE_SUCCESS);

    kvtree_elem* elem = NULL;
    char* dest;

    /* create destination directories for each file */
    if (make_directories) {
        while ((elem = axl_get_next_path(id, elem, NULL, &dest))) {
            char* dest_path = strdup(dest);
            char* dest_dir = dirname(dest_path);
            mode_t mode_dir = axl_getmode(1, 1, 1);
            axl_mkdir(dest_dir, mode_dir);
            axl_free(&dest_path);
        }
    }

#ifdef HAVE_BBAPI
    /* Special case: The BB API checks if the destination path exists at
     * its equivalent of AXL_Add() time.  That's why we do its "AXL_Add"
     * here, after the full path to the file has been created. */
    if (xtype == AXL_XFER_ASYNC_BBAPI) {
        /* Set if we're in BBAPI fallback mode */
        if (axl_all_paths_are_bbapi_compatible(id)) {
             kvtree_util_set_int(file_list, AXL_BBAPI_KEY_FALLBACK, 0);
        } else {
             kvtree_util_set_int(file_list, AXL_BBAPI_KEY_FALLBACK, 1);
        }

        if (!axl_bbapi_in_fallback(id) && !resume) {
            /* We're in regular BBAPI mode.  Add the paths before we transfer
             * them. */
            elem = NULL;
            char* src = NULL;
            while ((elem = axl_get_next_path(id, elem, &src, &dest))) {
                int bb_rc = axl_async_add_bbapi(id, src, dest);
                if (bb_rc != AXL_SUCCESS) {
                    return bb_rc;
                }
            }
        }
    }
#endif /* HAVE_BBAPI */

    if (!resume) {
        if (axl_save_metadata(id) != 0) {
            AXL_ERR("Couldn't save metadata");
            return AXL_FAILURE;
        }
        axl_write_state_file(id);
    }

    /* NOTE FOR XFER INTERFACES
     * each interface should update AXL_KEY_STATUS
     * all well as AXL_KEY_FILE_STATUS for each file */
    int rc = AXL_SUCCESS;
    switch (xtype) {
    case AXL_XFER_SYNC:
        if (resume) {
            rc = axl_sync_resume(id);
        } else {
            rc = axl_sync_start(id);
        }
        break;

#ifdef HAVE_PTHREADS
    case AXL_XFER_PTHREAD:
        if (resume) {
            rc = axl_pthread_resume(id);
        } else {
            rc = axl_pthread_start(id);
        }
        break;
#endif /* HAVE_PTHREADS */

#ifdef HAVE_BBAPI
    case AXL_XFER_ASYNC_BBAPI:
        if (resume) {
            rc = axl_async_resume_bbapi(id);
        } else {
            rc = axl_async_start_bbapi(id);
        }
        break;
#endif /* HAVE_BBAPI */

#ifdef HAVE_DATAWARP
    case AXL_XFER_ASYNC_DW:
        if (resume) {
            AXL_ERR("AXL_Resume() isn't supported yet for DW");
            rc = AXL_FAILURE;
            break;
        }
        rc = axl_async_start_datawarp(id);
        break;
#endif /* HAVE_DATAWARP */

    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    /* write data to file if we have one */
    axl_write_state_file(id);

    return rc;
}

int AXL_Dispatch(int id)
{
    return __AXL_Dispatch(id, 0);
}

int AXL_Resume(int id)
{
    return __AXL_Dispatch(id, 1);
}

/* Given a path with an AXL temporary extension, allocate an return a new
 * string with the extension removed.  Also, if extra is specified, return
 * a pointer to the offset in 'path_with_extension' where the 'extra' field
 * is.
 *
 * Returns the new allocated path string on success, or NULL on error. */
static char* axl_remove_extension(char* path_with_extension, char** extra)
{
    char* ext = AXL_EXTENSION;
    size_t ext_len = sizeof(AXL_EXTENSION) - 1; /* -1 for '\0' */

    /* path should at the very least be a one char file name + extension */
    size_t size = strlen(path_with_extension);
    if (size < 1 + ext_len) {
        return NULL;
    }

    /* Look backwards from the end of the string for the start of the
     * extension. */
    int i;
    for (i = size - ext_len; i >= 0; i--) {
        if (memcmp(&path_with_extension[i], ext, strlen(AXL_EXTENSION)) == 0) {
            /* Match! */
            if (extra) {
                *extra = &path_with_extension[i] + ext_len;
            }
            return strndup(path_with_extension, i);
        }
    }
    return NULL;
}

/* When you do an AXL transfer, it actually transfers to a temporary file
 * behind the scenes.  It's only after the transfer is finished that the file
 * is renamed to its final name.
 *
 * This function renames all the temporary files to their final names.  We
 * assume you're calling this after all the transfers have been successfully
 * transferred.
 *
 * TODO: Make the file renames multithreaded */
static int axl_rename_files_to_final_names(int id)
{
    int rc = AXL_SUCCESS;

    char* dst;
    kvtree_elem* elem = NULL;
    while ((elem = axl_get_next_path(id, elem, NULL, &dst))) {
        /* compute and allocate original name to store in newdst */
        char* extra = NULL;
        char* newdst = axl_remove_extension(dst, &extra);
        if (! newdst) {
            /* Nothing we can do... */
            AXL_ERR("Couldn't remove extension, this shouldn't happen");
            continue;
        }

        /* rename from temporary to final name */
        int tmp_rc = rename(dst, newdst);
        if (tmp_rc != 0) {
            AXL_ERR("Failed to rename file: `%s' to `%s' errno=%d %s",
                dst, newdst, errno, strerror(errno)
            );
            rc = AXL_FAILURE;
        }

        free(newdst);
    }

    return rc;
}

/* Test if a transfer has completed
 * Returns AXL_SUCCESS if the transfer has completed */
int AXL_Test (int id)
{
    int rc = AXL_SUCCESS;

    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to test */
    if (xstate != AXL_XFER_STATE_DISPATCHED) {
        AXL_ERR("Invalid state to test UID %d", id);
        return AXL_FAILURE;
    }

    int status;
    kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);
    if (status == AXL_STATUS_DEST) {
        goto end;
    } else if (status == AXL_STATUS_ERROR) {
        /* we return success since it's done, even on error,
         * caller must call wait to determine whether it was successful */
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_SOURCE) {
        AXL_ERR("Testing a transfer which was never started UID=%d", id);
        return AXL_FAILURE;
    } /* else (status == AXL_STATUS_INPROG) send to XFER interfaces */

    switch (xtype) {
    case AXL_XFER_SYNC:
        rc = axl_sync_test(id);
        break;

#ifdef HAVE_PTHREADS
    case AXL_XFER_PTHREAD:
        rc = axl_pthread_test(id);
        break;
#endif /* HAVE_PTHREADS */

#ifdef HAVE_BBAPI
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_test_bbapi(id);
        break;
#endif /* HAVE_BBAPI */

#ifdef HAVE_DATAWARP
    case AXL_XFER_ASYNC_DW:
        rc = axl_async_test_datawarp(id);
        break;
#endif /* HAVE_DATAWARP */

    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

end:
    return rc;
}

/* BLOCKING
 * Wait for a transfer to complete */
int AXL_Wait (int id)
{
    int rc = AXL_SUCCESS;

    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to wait */
    if (xstate != AXL_XFER_STATE_DISPATCHED) {
        AXL_ERR("Invalid state to wait UID %d", id);
        return AXL_FAILURE;
    }
    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_WAITING);

    /* lookup status for the transfer, return if done */
    int status;
    kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);

    if (status == AXL_STATUS_DEST) {
        kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_COMPLETED);
        goto end;
    } else if (status == AXL_STATUS_ERROR) {
        return AXL_FAILURE;
    } else if (status == AXL_STATUS_SOURCE) {
        AXL_ERR("Testing a transfer which was never started UID=%d", id);
        return AXL_FAILURE;
    } /* else (status == AXL_STATUS_INPROG) send to XFER interfaces */

    /* if not done, call vendor API to wait */
    switch (xtype) {
    case AXL_XFER_SYNC:
        rc = axl_sync_wait(id);
        break;

#ifdef HAVE_PTHREADS
    case AXL_XFER_PTHREAD:
        rc = axl_pthread_wait(id);
        break;
#endif /* HAVE_PTHREADS */

#ifdef HAVE_BBAPI
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_wait_bbapi(id);
        break;
#endif /* HAVE_BBAPI */

#ifdef HAVE_DATAWARP
    case AXL_XFER_ASYNC_DW:
        rc = axl_async_wait_datawarp(id);
        break;
#endif /* HAVE_DATAWARP */

    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

end:
    /* Are all our destination files the correct size? */
    rc = axl_check_file_sizes(id);

    /* Set permissions and creation times on files */
    if (rc == AXL_SUCCESS && axl_copy_metadata) {
        rc = axl_set_metadata(id);
    }

    /* if we're successful, rename temporary files to final destination names */
    if (rc == AXL_SUCCESS && axl_use_extension) {
        rc = axl_rename_files_to_final_names(id);
    }

    /* if anything failed, be sure to mark transfer status as being in error */
    if (rc != AXL_SUCCESS) {
        kvtree_util_set_int(file_list, AXL_KEY_STATUS, AXL_STATUS_ERROR);
    }

    /* TODO: if error, delete destination files including temporaries? */

    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_COMPLETED);

    /* write data to file if we have one */
    axl_write_state_file(id);

    return rc;
}

/* Cancel an existing transfer */
/* TODO: Does cancel call free? */
int AXL_Cancel (int id)
{
    int rc = AXL_SUCCESS;

    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to cancel */
    if (xstate != AXL_XFER_STATE_DISPATCHED &&
        xstate != AXL_XFER_STATE_WAITING)
    {
        AXL_ERR("Invalid state to cancel UID %d, state = %d", id, xstate);
        return AXL_FAILURE;
    }

    /* lookup status for the transfer, return if done */
    int status;
    kvtree_util_get_int(file_list, AXL_KEY_STATUS, &status);
    if (status == AXL_STATUS_DEST) {
        return AXL_SUCCESS;
    } else if (status == AXL_STATUS_ERROR) {
        /* we return success since it's done, even on error */
        return AXL_SUCCESS;
    }

    /* TODO: if it hasn't started, we don't want to call backend cancel */

    /* if not done, call vendor API to wait */
    switch (xtype) {
/* TODO: add cancel to backends */
#if 0
    case AXL_XFER_SYNC:
        rc = axl_sync_cancel(id);
        rc = AXL_FAILURE;
        break;
#endif

#ifdef HAVE_PTHREADS
    case AXL_XFER_PTHREAD:
        rc = axl_pthread_cancel(id);
        break;
#endif /* HAVE_PTHREADS */

#ifdef HAVE_BBAPI
    case AXL_XFER_ASYNC_BBAPI:
        rc = axl_async_cancel_bbapi(id);
        break;
#endif /* HAVE_BBAPI */

#if 0
    case AXL_XFER_ASYNC_DW:
        rc = axl_async_cancel_datawarp(id);
        break;
#endif

    default:
        AXL_ERR("Unknown transfer type (%d)", (int) xtype);
        rc = AXL_FAILURE;
        break;
    }

    kvtree_util_set_int(file_list, AXL_KEY_STATE, (int)AXL_XFER_STATE_CANCELED);

    /* write data to file if we have one */
    axl_write_state_file(id);

    return rc;
}

/* Perform cleanup of internal data associated with ID */
int AXL_Free (int id)
{
    /* lookup transfer info for the given id */
    kvtree* file_list = NULL;
    axl_xfer_t xtype = AXL_XFER_NULL;
    axl_xfer_state_t xstate = AXL_XFER_STATE_NULL;
    if (axl_get_info(id, &file_list, &xtype, &xstate) != AXL_SUCCESS) {
        AXL_ERR("Could not find transfer info for UID %d", id);
        return AXL_FAILURE;
    }

    /* check that handle is in correct state to free */
    if (xstate != AXL_XFER_STATE_CREATED &&
        xstate != AXL_XFER_STATE_COMPLETED &&
        xstate != AXL_XFER_STATE_CANCELED)
    {
        AXL_ERR("Invalid state to free UID %d", id);
        return AXL_FAILURE;
    }

#ifdef HAVE_PTHREADS
    if (xtype == AXL_XFER_PTHREAD) {
        axl_pthread_free(id);
    }
#endif

    /* write data to file if we have one */
    axl_write_state_file(id);

    /* forget anything we know about this id */
    axl_free_id(id);

    return AXL_SUCCESS;
}

int AXL_Stop ()
{
    int rc = AXL_SUCCESS;

    /* cancel each active id */
    int id;
    for (id = 0; id < axl_kvtrees_count; id++) {
        if (!axl_kvtrees[id]) {
            continue;
        }

        if (AXL_Cancel(id) != AXL_SUCCESS) {
            rc = AXL_FAILURE;
        }
    }

    /* wait */
    for (id = 0; id < axl_kvtrees_count; id++) {
        if (!axl_kvtrees[id]) {
            continue;
        }

        if (AXL_Wait(id) != AXL_SUCCESS) {
            rc = AXL_FAILURE;
        }
    }

    /* and free it */
    for (id = 0; id < axl_kvtrees_count; id++) {
        if (!axl_kvtrees[id]) {
            continue;
        }

        if (AXL_Free(id) != AXL_SUCCESS) {
            rc = AXL_FAILURE;
        }
    }

    return rc;
}
