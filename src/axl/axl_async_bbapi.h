#ifndef AXL_ASYNC_BBAPI_H
#define AXL_ASYNC_BBAPI_H
#include <stdint.h>

#define AXL_BBAPI_KEY_TRANSFERHANDLE ("BB_TransferHandle")
#define AXL_BBAPI_KEY_TRANSFERDEF ("BB_TransferDef")
#define AXL_BBAPI_KEY_FALLBACK ("BB_Fallback")

/** \file axl_async_bbapi.h
 *  \ingroup axl
 *  \brief implementation of axl for IBM bbapi */

/** \name bbapi */
///@{
int axl_async_init_bbapi(void);
int axl_async_finalize_bbapi(void);
int axl_async_create_bbapi(int id);
int axl_async_add_bbapi(int id, const char* source, const char* destination);
int axl_async_get_bbapi_handle(int id, uint64_t* thandle);
int axl_async_start_bbapi(int id);
int axl_async_resume_bbapi(int id);
int axl_async_test_bbapi(int id);
int axl_async_wait_bbapi(int id);
int axl_async_cancel_bbapi(int id);

int axl_all_paths_are_bbapi_compatible(int id);

/*
 * If the BBAPI is in fallback mode return 1, else return 0.
 *
 * Fallback mode happens when we can't transfer the files using the BBAPI due
 * to lack of filesystem support.  Instead, fall back to a more compatible
 * transfer mode.
 */
int axl_bbapi_in_fallback(int id);

///@}
#endif //AXL_ASYNC_BBAPI_H
