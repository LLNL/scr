#ifndef AXL_PTHREAD_H
#define AXL_PTHREAD_H

/** \file axl_pthread.h
 *  \ingroup axl
 *  \brief implementation of axl using pthreads */

/** \name pthread */
///@{
int axl_pthread_start(int id);
int axl_pthread_resume(int id);
int axl_pthread_test(int id);
int axl_pthread_wait(int id);
int axl_pthread_cancel(int id);
void axl_pthread_free(int id);
///@}
#endif //AXL_PTHREAD_H
