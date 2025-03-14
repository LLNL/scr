#ifndef KVTREE_ERR_H
#define KVTREE_ERR_H

/*
=========================================
Error and Debug Messages
=========================================
*/

/* print error message to stdout */
void kvtree_err(const char *fmt, ...);

/* print warning message to stdout */
void kvtree_warn(const char *fmt, ...);

/* print message to stdout if kvtree_debug is set and it is >= level */
void kvtree_dbg(int level, const char *fmt, ...);

/* print abort message and kill run */
void kvtree_abort(int rc, const char *fmt, ...);

#endif
