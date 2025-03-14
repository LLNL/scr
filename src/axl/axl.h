#ifndef AXL_H
#define AXL_H

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#define AXL_SUCCESS (0)

#define AXL_VERSION "0.9.0"

/** \defgroup axl AXL
 *  \brief Asynchronous Transfer Library
 *
 * AXL is used to transfer a file from one path to another using
 * synchronous and asynchronous methods. This can only be done between
 * storage tiers, AXL does not (yet) support movement within a storage
 * tier (such as between 2 compute nodes). Asynchronous methods
 * include via pthreads, IBM BB API, Cray Datawarp. AXL will create
 * directories for destination files. */

/** \file axl.h
 *  \ingroup axl
 *  \brief asynchronous transfer library */

/** AXL configuration options */
#define AXL_KEY_CONFIG_FILE_BUF_SIZE "FILE_BUF_SIZE"
#define AXL_KEY_CONFIG_DEBUG "DEBUG"
#define AXL_KEY_CONFIG_MKDIR "MKDIR"
#define AXL_KEY_CONFIG_USE_EXTENSION "USE_EXTENSION"
#define AXL_KEY_CONFIG_COPY_METADATA "COPY_METADATA"
#define AXL_KEY_CONFIG_RANK "RANK"

/** Supported AXL transfer methods
 * Note that DW, and BBAPI must be found at compile time */
typedef enum {
    AXL_XFER_NULL = 0,      /* placeholder to represent invalid value */
    AXL_XFER_DEFAULT,       /* Autodetect and use the fastest API for this node
                             * type that supports all AXL transfers.
                             */
    AXL_XFER_SYNC,          /* synchronous copy */
    AXL_XFER_ASYNC_DAEMON,  /* async daemon process (not used, but kept to maintain enum values) */
    AXL_XFER_ASYNC_DW,      /* Cray Datawarp */
    AXL_XFER_ASYNC_BBAPI,   /* IBM Burst Buffer API */
    AXL_XFER_NATIVE,        /* Autodetect and use the native API (BBAPI, DW,
                             * etc) for this node type.  It may or may not
                             * be compatible with all AXL transfers (like
                             * shmem).  If there is no native API, fall back
                             * to AXL_XFER_DEFAULT.
                             */
    AXL_XFER_PTHREAD,      /* parallel copy using pthreads */
    AXL_XFER_STATE_FILE,    /* Use the xfer type specified in the state_file. */
} axl_xfer_t;

/*
 * int AXL_Init(void) - Initialize the library.
 */
int AXL_Init (void);

/** Shutdown any vendor services */
int AXL_Finalize (void);

/*
 * int AXL_Create(type, name, state_file) - Create a transfer handle to copy files
 *
 * type:        Transfer type to use
 * name:        A unique tag to give to this transfer handle
 * state_file:  (optional) Path to store our transfer state.  This is needed to
 *              resume transfers after a crash.
 *
 * Returns an AXL ID, or negative number on error.
 */
int AXL_Create (axl_xfer_t xtype, const char* name, const char* state_file);

/* needs to be above doxygen comment to get association right */
typedef struct kvtree_struct kvtree;

/**
 * Get/set AXL configuration values.
 *
 * config: The new configuration.  Global variables are in top level of
 *              the tree, and per-ID values are subtrees.  If config=NULL,
 *              then return a kvtree with all the configuration values (globals
 *              and all per-ID trees).
 *
 * Thread safety: setting the DEBUG or any per-transfer configuration value
 * after the transfer has been dispatched entails a race contion between the
 * main thread and the worker threads. Changing configuration options after a
 * transfer has been dispatched is not supported.
 *
 * Return value: If config != NULL, then return config on success.  If
 *                      config=NULL (you're querying the config) then return
 *                      a new kvtree on success.  Return NULL on any failures.
 */
kvtree* AXL_Config(
  const kvtree* config        /** [IN] - kvtree of options */
);

/** Add a file to an existing transfer handle */
int AXL_Add (int id, const char* source, const char* destination);

/** Initiate a transfer for all files in handle ID */
int AXL_Dispatch (int id);

/**
 * AXL_Resume works the same as AXL_Dispatch(), but resumes any transfers where
 * they left off.  In the case of a BB API transfer, it will leave ongoing
 * transfers running.  If there are no ongoing or canceled transfers,
 * AXL_Resume() behaves the same as an AXL_Dispatch().
 *
 * AXL_Resume() is typically used in conjunction with passing an existing
 * state_file is passed to AXL_Create().
 * */
int AXL_Resume (int id);

/** Non-blocking call to test if a transfer has completed,
 * returns AXL_SUCCESS if the transfer has completed,
 * does not indicate whether transfer was successful,
 * only whether it's done */
int AXL_Test (int id);

/** BLOCKING
 * Wait for a transfer to complete,
 * and finalize the transfer */
int AXL_Wait (int id);

/** Cancel an existing transfer, must call Wait
 * on cancelled transfers, if cancelled wait returns an error */
int AXL_Cancel (int id);

/** Perform cleanup of internal data associated with ID */
int AXL_Free (int id);

/** Stop (cancel and free) all transfers,
 * useful to clean the plate when restarting */
int AXL_Stop (void);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
