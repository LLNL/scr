#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <sys/stat.h>
#include <signal.h>
#include <stdlib.h>
#include "axl.h"

int id = -1;
char *old_env = NULL;

/* Return 1 if path is a directory */
static int
is_dir(const char *path) {
   struct stat s;
   if (stat(path, &s) != 0)
       return 0;
   return S_ISDIR(s.st_mode);
}

/* Translate a string like "bbapi" to its corresponding axl_xfer_t */
static axl_xfer_t
axl_xfer_str_to_xfer(const char *xfer_str)
{
    struct {
        const char *xfer_str;
        axl_xfer_t xfer;
    } xfer_strs[] = {
            {"default", AXL_XFER_DEFAULT},
            {"native", AXL_XFER_NATIVE},
            {"sync", AXL_XFER_SYNC},
            {"dw", AXL_XFER_ASYNC_DW},
            {"bbapi", AXL_XFER_ASYNC_BBAPI},
            {"pthread", AXL_XFER_PTHREAD},
            {"state_file", AXL_XFER_STATE_FILE},
            {NULL, AXL_XFER_NULL},  /* must always be last element in array */
    };
    int i;

    for (i = 0; xfer_strs[i].xfer_str; i++) {
        if (strcmp(xfer_strs[i].xfer_str, xfer_str) == 0) {
            /* Match */
            return xfer_strs[i].xfer;
        }
    }
    /* No match */
    return AXL_XFER_NULL;
}

static void
usage(void)
{
    printf("Usage: axl_cp [-ap] [-r|-R] [-S state_file [-U]] [-X xfer_type] SOURCE DEST\n");
    printf("       axl_cp [-ap] [-r|-R] [-S state_file [-U]] [-X xfer_type] SOURCE... DIRECTORY\n");
    printf("\n");
    printf("-a:             Archive mode.  Preserve permissions + times + recursive.  Implies -pr\n");
    printf("-p:             Preserve permissions + times.\n");
    printf("-r|-R:          Copy directories recursively\n");
    printf("-S state_file:  Reload state from state_file\n");
    printf("-U:             Resume copies to existing destination files if they exist\n");
    printf("-X xfer_type:   AXL transfer type: default native pthread sync dw bbapi state_file.\n");
    printf("\n");
}

void sig_func(int signum)
{
    int rc;
    if (id == -1) {
        /* AXL not initialized yet */
        return;
    }

    rc = AXL_Cancel(id);
    if (rc != AXL_SUCCESS) {
        printf("axl_cp SIGTERM: AXL_Cancel failed (%d)", rc);
        exit(rc);
    }

    rc = AXL_Free(id);
    if (rc != AXL_SUCCESS) {
        printf("axl_cp SIGTERM: AXL_Free failed (%d)", rc);
        exit(rc);
    }

    rc = AXL_Finalize();
    if (rc != AXL_SUCCESS) {
        printf("axl_cp SIGTERM: AXL_Finalized failed (%d)", rc);
        exit(rc);
    }

    if (old_env)
        setenv("AXL_COPY_METADATA", old_env, 1);

    exit(AXL_SUCCESS);
}

int
main(int argc, char **argv) {
    int rc;
    int opt;
    int args_left;
    const char *xfer_str = NULL;
    char **src = NULL;
    char *dest = NULL;
    axl_xfer_t xfer;
    unsigned int src_count;
    int i;
    int recursive = 0, resume = 0;
    struct sigaction action;

    memset(&action, 0, sizeof(action));
    action.sa_handler = sig_func;
    sigaction(SIGTERM, &action, NULL);
    char *state_file = NULL;
    int preserve = 0;

    while ((opt = getopt(argc, argv, "aprRS:UX:")) != -1) {
        switch (opt) {
            case 'a':
                preserve = 1;
                recursive = 1;
                break;
            case 'p':
                preserve = 1;
                break;
            case 'r':
            case 'R':
                recursive = 1;
                break;
            case 'U':
                resume = 1;
                break;
            case 'S':
                state_file = optarg;
                break;
            case 'X':
                xfer_str = optarg;
                break;
            default: /* '?' */
                usage();
                exit(1);
        }
    }

    if (preserve)
       old_env = getenv("AXL_COPY_METADATA");

    args_left = argc - optind;

    /* Did they specify source(s) and destination? */
    if (!resume && args_left == 0) {
        printf("Missing source and destination\n");
        usage();
        exit(1);
    } else if (!resume && args_left == 1) {
        printf("Missing destination");
        usage();
        exit(1);
    } else {
        src = &argv[optind];
        src_count = args_left - 1;
        dest = argv[optind + args_left - 1];
    }

    /*
     * They're doing a copy where the final arg is a destination directory,
     * not a file.
     */
    if (args_left > 2 && !is_dir(dest)) {
        printf("axl_cp: target '%s' is not a directory\n", dest);
        exit(1);
    }

    if (xfer_str) {
        xfer = axl_xfer_str_to_xfer(xfer_str);
        if (xfer == AXL_XFER_NULL) {
            printf("Error: Invalid AXL transfer type '%s'\n", xfer_str);
            printf("\n");
            usage();
            exit(1);
        }
    } else {
        xfer = AXL_XFER_SYNC;
    }

    if (preserve)
        setenv("AXL_COPY_METADATA", "1", 1);

    rc = AXL_Init();
    if (rc != AXL_SUCCESS) {
        printf("AXL_Init() failed (error %d)\n", rc);
        return rc;
    }

    id = AXL_Create(xfer, "axl_cp", state_file);
    if (id == -1) {
        printf("AXL_Create() failed (error %d)\n", id);
        return id;
    }

    if (resume) {
        rc = AXL_Resume(id);
    } else {
        /* Starting a new file list */
        for (i = 0; i < src_count; i++) {
            if (!recursive && is_dir(src[i])) {
                printf("axl_cp: omitting directory '%s'\n", src[i]);
                continue;
            }
            rc = AXL_Add(id, src[i], dest);
            if (rc != AXL_SUCCESS) {
                printf("AXL_Add(..., %s, %s) failed (error %d)\n", src[i], dest, rc);
                return rc;
            }
        }

        rc = AXL_Dispatch(id);
    }

    if (rc != AXL_SUCCESS) {
        printf("AXL_Dispatch()/AXL_Resume() failed (error %d)\n", rc);
        return rc;
    }

    /* Wait for transfer to complete and finalize axl */
    rc = AXL_Wait(id);
    if (rc != AXL_SUCCESS) {
        printf("AXL_Wait() failed (error %d)\n", rc);
        return rc;
    }

    rc = AXL_Free(id);
    if (rc != AXL_SUCCESS) {
        printf("AXL_Free() failed (error %d)\n", rc);
        return rc;
    }

    rc = AXL_Finalize();
    if (rc != AXL_SUCCESS) {
        printf("AXL_Finalize() failed (error %d)\n", rc);
        return rc;
    }

    if (old_env)
        setenv("AXL_COPY_METADATA", old_env, 1);

    return AXL_SUCCESS;
}
