#ifndef AXL_SYNC_H
#define AXL_SYNC_H

/** \file axl_sync.h
 *  \ingroup axl
 *  \brief implementation of axl's synchronous mode */

/** \name sync */
///@{
int axl_sync_start(int id);
int axl_sync_test(int id);
int axl_sync_wait(int id);
int axl_sync_resume(int id);
///@}
#endif //AXL_SYNC_H
