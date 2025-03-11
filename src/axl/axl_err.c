/* variable length args */
#include <stdarg.h>

/* stdout & stderr */
#include <stdio.h>

/* exit */
#include <stdlib.h>

/* gethostname */
#include <unistd.h>

/* axl version */
#include "axl.h"

/* current debug level for AXL library,
 * set in AXL_Init used in axl_dbg */
int axl_debug;

/* print message to stdout if axl_debug is set and it is >= level */
void axl_dbg(int level, const char* fmt, ...)
{
    /* get my hostname */
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        /* TODO: error! */
    }
  
    va_list argp;
    if (level == 0 || (axl_debug > 0 && axl_debug >= level)) {
        fprintf(stdout, "AXL %s: %s: ", AXL_VERSION, hostname);
        va_start(argp, fmt);
        vfprintf(stdout, fmt, argp);
        va_end(argp);
        fprintf(stdout, "\n");
    }
}

/* print error message to stdout */
void axl_err(const char* fmt, ...)
{
    /* get my hostname */
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        /* TODO: error! */
    }
  
    va_list argp;
    fprintf(stdout, "AXL %s ERROR: %s: ", AXL_VERSION, hostname);
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
    fprintf(stdout, "\n");
}

/* print abort message and kill run */
void axl_abort(int rc, const char* fmt, ...)
{
    /* get my hostname */
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) != 0) {
        /* TODO: error! */
    }
  
    va_list argp;
    fprintf(stderr, "AXL %s ABORT: %s: ", AXL_VERSION, hostname);
    va_start(argp, fmt);
    vfprintf(stderr, fmt, argp);
    va_end(argp);
    fprintf(stderr, "\n");
  
    exit(rc);
}
