#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "axl.h"
#include "axl_internal.h"

#include "kvtree.h"
#include "kvtree_util.h"

/* settable option values */
size_t old_axl_file_buf_size;
int old_axl_debug;
int old_axl_make_directories;
int old_axl_use_extension;
int old_axl_copy_metadata;
int old_axl_rank;

/* values that options were set to */
size_t new_axl_file_buf_size;
int new_axl_debug;
int new_axl_make_directories;
int new_axl_use_extension;
int new_axl_copy_metadata;
int new_axl_rank;

/* tests setting global options, error exits if failure are detected */
void set_global_options(void)
{
    int rc;
    kvtree* axl_config_values = kvtree_new();

    /* check AXL configuration settings */
    new_axl_file_buf_size = old_axl_file_buf_size + 1;
    rc = kvtree_util_set_bytecount(axl_config_values,
                                   AXL_KEY_CONFIG_FILE_BUF_SIZE,
                                   new_axl_file_buf_size);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_bytecount failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    new_axl_debug = !old_axl_debug;
    rc = kvtree_util_set_int(axl_config_values, AXL_KEY_CONFIG_DEBUG,
                             new_axl_debug);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    printf("Configuring AXL (first set of options)...\n");
    if (AXL_Config(axl_config_values) == NULL) {
        printf("AXL_Config() failed\n");
        exit(EXIT_FAILURE);
    }

    /* check that options were set */

    if (axl_file_buf_size != new_axl_file_buf_size) {
        printf("AXL_Config() failed to set %s: %lu != %lu\n",
               AXL_KEY_CONFIG_FILE_BUF_SIZE, (long unsigned)axl_file_buf_size,
               (long unsigned)(new_axl_file_buf_size));
        exit(EXIT_FAILURE);
    }

    if (axl_debug != new_axl_debug) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_DEBUG, axl_debug, new_axl_debug);
        exit(EXIT_FAILURE);
    }

    /* set remainder of options */
    kvtree_delete(&axl_config_values);
    axl_config_values = kvtree_new();

    new_axl_make_directories = !old_axl_make_directories;
    rc = kvtree_util_set_int(axl_config_values, AXL_KEY_CONFIG_MKDIR,
                             new_axl_make_directories);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    new_axl_use_extension = !old_axl_use_extension;
    rc = kvtree_util_set_int(axl_config_values, AXL_KEY_CONFIG_USE_EXTENSION,
                             new_axl_use_extension);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    new_axl_copy_metadata = !old_axl_copy_metadata;
    rc = kvtree_util_set_int(axl_config_values, AXL_KEY_CONFIG_COPY_METADATA,
                             new_axl_copy_metadata);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    new_axl_rank = old_axl_rank + 1;
    rc = kvtree_util_set_int(axl_config_values, AXL_KEY_CONFIG_RANK,
                             new_axl_rank);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    printf("Configuring AXL (second set of options)...\n");
    if (AXL_Config(axl_config_values) == NULL) {
        printf("AXL_Config() failed\n");
        exit(EXIT_FAILURE);
    }

    /* check all options once more */

    /* check that all expected global variables were set */
    if (axl_file_buf_size != new_axl_file_buf_size) {
        printf("AXL_Config() failed to set %s: %lu != %lu\n",
               AXL_KEY_CONFIG_FILE_BUF_SIZE, (long unsigned)axl_file_buf_size,
               (long unsigned)(new_axl_file_buf_size));
        exit(EXIT_FAILURE);
    }

    if (axl_debug != new_axl_debug) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_DEBUG, axl_debug, new_axl_debug);
        exit(EXIT_FAILURE);
    }

    if (axl_make_directories != new_axl_make_directories) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_MKDIR, axl_make_directories,
               new_axl_make_directories);
        exit(EXIT_FAILURE);
    }

    if (axl_use_extension != new_axl_use_extension) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_USE_EXTENSION, axl_use_extension,
               new_axl_use_extension);
        exit(EXIT_FAILURE);
    }

    if (axl_copy_metadata != new_axl_copy_metadata) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_COPY_METADATA, axl_copy_metadata,
               new_axl_copy_metadata);
        exit(EXIT_FAILURE);
    }

    if (axl_rank != new_axl_rank) {
        printf("AXL_Config() failed to set %s: %d != %d\n",
               AXL_KEY_CONFIG_RANK, axl_rank,
               new_axl_rank);
        exit(EXIT_FAILURE);
    }

    kvtree_delete(&axl_config_values);
}

/* helper function to check for known options */
void check_known_options(const kvtree* configured_values,
                         int ignore_id_option,
                         const char* known_options[])
{
    /* report all unknown options (typos?) */
    const kvtree_elem* elem;
    for (elem = kvtree_elem_first(configured_values);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
        const char* key = kvtree_elem_key(elem);

        if (ignore_id_option && strcmp("id", key) == 0)
            continue;

        /* must be only one level deep, ie plain kev = value */
        const kvtree* elem_hash = kvtree_elem_hash(elem);
        if (kvtree_size(elem_hash) != 1) {
            printf("Element %s has unexpected number of values: %d", key,
                   kvtree_size(elem_hash));
            exit(EXIT_FAILURE);
        }

        const kvtree* kvtree_first_elem_hash =
          kvtree_elem_hash(kvtree_elem_first(elem_hash));
        if (kvtree_size(kvtree_first_elem_hash) != 0) {
            printf("Element %s is not a pure value", key);
            exit(EXIT_FAILURE);
        }

        /* check against known options */
        const char** opt;
        int found = 0;
        for (opt = known_options; *opt != NULL; opt++) {
            if (strcmp(*opt, key) == 0) {
                found = 1;
                break;
            }
        }
        if (! found) {
            printf("Unknown configuration parameter '%s' with value '%s'\n",
              kvtree_elem_key(elem),
              kvtree_elem_key(kvtree_elem_first(kvtree_elem_hash(elem)))
            );
            exit(EXIT_FAILURE);
        }
    }
}

/* helper function to compare given configuration values with a kvtree */
void check_options(const kvtree* configured_values, int is_global,
                   size_t exp_file_buf_size, int exp_debug,
                   int exp_make_directories, int exp_use_extension, int exp_copy_metadata,
                   int exp_rank)
{
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
    const char** known_options = is_global ? known_global_options :
                                             known_transfer_options;

    unsigned long ul;
    if (kvtree_util_get_bytecount(configured_values,
      AXL_KEY_CONFIG_FILE_BUF_SIZE, &ul) != KVTREE_SUCCESS)
    {
        printf("Could not get %s from AXL_Config\n",
               AXL_KEY_CONFIG_FILE_BUF_SIZE);
        exit(EXIT_FAILURE);
    }
    size_t cfg_file_buf_size = (size_t) ul;
    if (cfg_file_buf_size != ul) {
        char* value;
        kvtree_util_get_str(configured_values, AXL_KEY_CONFIG_FILE_BUF_SIZE,
                            &value);
        printf("Value '%s' passed for %s exceeds int range\n",
          value, AXL_KEY_CONFIG_FILE_BUF_SIZE
        );
        exit(EXIT_FAILURE);
    }
    if (cfg_file_buf_size != exp_file_buf_size) {
        printf("AXL_Config returned unexpected value %llu for %s. Expected %llu.\n",
               (unsigned long long)cfg_file_buf_size,
               AXL_KEY_CONFIG_FILE_BUF_SIZE,
               (unsigned long long)exp_file_buf_size);
        exit(EXIT_FAILURE);
    }
    
    if (is_global) {
        int cfg_debug;
        if (kvtree_util_get_int(configured_values, AXL_KEY_CONFIG_DEBUG,
                                &cfg_debug) != KVTREE_SUCCESS)
        {
            printf("Could not get %s from AXL_Config\n",
                   AXL_KEY_CONFIG_DEBUG);
            exit(EXIT_FAILURE);
        }
        if (cfg_debug != exp_debug) {
            printf("AXL_Config returned unexpected value %d for %s. Expected %d.\n",
                   cfg_debug, AXL_KEY_CONFIG_DEBUG, exp_debug);
            exit(EXIT_FAILURE);
        }

        int cfg_rank;
        if (kvtree_util_get_int(configured_values, AXL_KEY_CONFIG_RANK,
                                &cfg_rank) != KVTREE_SUCCESS)
        {
            printf("Could not get %s from AXL_Config\n",
                   AXL_KEY_CONFIG_RANK);
            exit(EXIT_FAILURE);
        }
        if (cfg_rank != exp_rank) {
            printf("AXL_Config returned unexpected value %d for %s. Expected %d.\n",
                   cfg_rank, AXL_KEY_CONFIG_RANK,
                   exp_rank);
            exit(EXIT_FAILURE);
        }
    }

    int cfg_make_directories;
    if (kvtree_util_get_int(configured_values, AXL_KEY_CONFIG_MKDIR,
                            &cfg_make_directories) != KVTREE_SUCCESS)
    {
        printf("Could not get %s from AXL_Config\n",
               AXL_KEY_CONFIG_MKDIR);
        exit(EXIT_FAILURE);
    }
    if (cfg_make_directories != exp_make_directories) {
        printf("AXL_Config returned unexpected value %d for %s. Expected %d.\n",
               cfg_make_directories, AXL_KEY_CONFIG_MKDIR,
               exp_make_directories);
        exit(EXIT_FAILURE);
    }

    int cfg_use_extension;
    if (kvtree_util_get_int(configured_values, AXL_KEY_CONFIG_USE_EXTENSION,
                            &cfg_use_extension) != KVTREE_SUCCESS)
    {
        printf("Could not get %s from AXL_Config\n",
               AXL_KEY_CONFIG_USE_EXTENSION);
        exit(EXIT_FAILURE);
    }
    if (cfg_use_extension != exp_use_extension) {
        printf("AXL_Config returned unexpected value %d for %s. Expected %d.\n",
               cfg_use_extension, AXL_KEY_CONFIG_USE_EXTENSION,
               exp_use_extension);
        exit(EXIT_FAILURE);
    }

    int cfg_copy_metadata;
    if (kvtree_util_get_int(configured_values, AXL_KEY_CONFIG_COPY_METADATA,
                            &cfg_copy_metadata) != KVTREE_SUCCESS)
    {
        printf("Could not get %s from AXL_Config\n",
               AXL_KEY_CONFIG_COPY_METADATA);
        exit(EXIT_FAILURE);
    }
    if (cfg_copy_metadata != exp_copy_metadata) {
        printf("AXL_Config returned unexpected value %d for %s. Expected %d.\n",
               cfg_copy_metadata, AXL_KEY_CONFIG_COPY_METADATA,
               exp_copy_metadata);
        exit(EXIT_FAILURE);
    }

    check_known_options(configured_values, is_global, known_options);
}

void get_global_options(void)
{
    kvtree *axl_configured_values = AXL_Config(NULL);
    if (axl_configured_values == NULL) {
        printf("AXL_Config() failed to get config\n");
        exit(EXIT_FAILURE);
    }

    check_options(axl_configured_values, 1, new_axl_file_buf_size,
                  new_axl_debug, new_axl_make_directories,
                  new_axl_use_extension, new_axl_copy_metadata, new_axl_rank);

    kvtree_delete(&axl_configured_values);
}

void set_transfer_options(int id, size_t file_buf_size, int make_directories,
                          int use_extension, int copy_metadata)
{
    int rc;

    kvtree* config = kvtree_new();
    if (config == NULL) {
        printf("kvtree_new() failed\n");
        exit(EXIT_FAILURE);
    }

    kvtree* transfer_config = kvtree_set_kv_int(config, "id", id);
    if (config == NULL) {
        printf("kvtree_kv_set_int(config, 'id', %d) failed\n", id);
        exit(EXIT_FAILURE);
    }

    rc = kvtree_util_set_bytecount(transfer_config,
                                   AXL_KEY_CONFIG_FILE_BUF_SIZE,
                                   file_buf_size);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_bytecount failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    rc = kvtree_util_set_int(transfer_config, AXL_KEY_CONFIG_MKDIR,
                             make_directories);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    rc = kvtree_util_set_int(transfer_config, AXL_KEY_CONFIG_USE_EXTENSION,
                             use_extension);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    rc = kvtree_util_set_int(transfer_config, AXL_KEY_CONFIG_COPY_METADATA,
                             copy_metadata);
    if (rc != KVTREE_SUCCESS) {
        printf("kvtree_util_set_int failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    if (AXL_Config(config) == NULL) {
        printf("AXL_Config() failed\n");
        exit(EXIT_FAILURE);
    }

    kvtree_delete(&config);
}

void get_transfer_options(int id, size_t file_buf_size, int make_directories,
                          int use_extension, int copy_metadata)
{
    kvtree* config = AXL_Config(NULL);
    if (config == NULL) {
        printf("AXL_Config() failed\n");
        exit(EXIT_FAILURE);
    }

    kvtree* transfer_config = kvtree_get_kv_int(config, "id", id);
    if (transfer_config == NULL) {
        printf("Could not get config for id %d\n", id);
        exit(EXIT_FAILURE);
    }

    /* check known option values */
    check_options(transfer_config, 0, file_buf_size, -1, make_directories,
                  use_extension, copy_metadata, -1);

    kvtree_delete(&config);
}

int
main(void) {
    int rc;

    rc = AXL_Init();
    if (rc != AXL_SUCCESS) {
        printf("AXL_Init() failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    old_axl_file_buf_size    = axl_file_buf_size;
    old_axl_debug            = axl_debug;
    old_axl_make_directories = axl_make_directories;
    old_axl_use_extension    = axl_use_extension;
    old_axl_copy_metadata    = axl_copy_metadata;
    old_axl_rank             = axl_rank;

    /* must pick up "old" defaults */
    int id1 = AXL_Create(AXL_XFER_DEFAULT, __FILE__, NULL);
    if (id1 < 0) {
        printf("AXL_Create() failed (error %d)\n", id1);
        exit(EXIT_FAILURE);
    }

    set_global_options();
    get_global_options();

    /* must pick up "new" defaults */
    int id2 = AXL_Create(AXL_XFER_DEFAULT, __FILE__, NULL);
    if (id2 < 0) {
        printf("AXL_Create() failed (error %d)\n", id2);
        exit(EXIT_FAILURE);
    }

    /* check that global values are used by default */
    get_transfer_options(id1, old_axl_file_buf_size, old_axl_make_directories,
        old_axl_use_extension, old_axl_copy_metadata);
    get_transfer_options(id2, new_axl_file_buf_size, new_axl_make_directories,
        new_axl_use_extension, new_axl_copy_metadata);

    /* change values */
    set_transfer_options(id1, new_axl_file_buf_size+1, new_axl_make_directories,
        new_axl_use_extension, new_axl_copy_metadata);
    /* did they change? */
    get_transfer_options(id1, new_axl_file_buf_size+1, new_axl_make_directories,
        new_axl_use_extension, new_axl_copy_metadata);
    /* but only for the one I did change? */
    get_transfer_options(id2, new_axl_file_buf_size, new_axl_make_directories,
        new_axl_use_extension, new_axl_copy_metadata);

    rc = AXL_Free(id2);
    if (rc != AXL_SUCCESS) {
        printf("AXL_Free(%d) failed (error %d)\n", id2, rc);
        exit(EXIT_FAILURE);
    }

    rc = AXL_Free(id1);
    if (rc != AXL_SUCCESS) {
        printf("AXL_Free(%d) failed (error %d)\n", id1, rc);
        exit(EXIT_FAILURE);
    }

    rc = AXL_Finalize();
    if (rc != AXL_SUCCESS) {
        printf("AXL_Finalize() failed (error %d)\n", rc);
        exit(EXIT_FAILURE);
    }

    return EXIT_SUCCESS;
}
