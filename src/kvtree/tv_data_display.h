/* 
 * $Header: /home/tv/src/debugger/src/datadisp/tv_data_display.h,v 1.5 2010/10/04 04:02:19 anb Exp $
 * $Locker:  $

   Copyright (c) 2010, Rogue Wave Software, Inc.

   Permission is hereby granted, free of charge, to any person obtaining a copy
   of this software and associated documentation files (the "Software"), to deal
   in the Software without restriction, including without limitation the rights
   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
   copies of the Software, and to permit persons to whom the Software is
   furnished to do so, subject to the following conditions:

   The above copyright notice and this permission notice shall be included in
   all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
   THE SOFTWARE.

 * Update log
 *
 * Sep 27 2010 ANB: reworked as part of totalview/12314
 * Jun 17 2010 JVD: Added TV_elide_row.
 * Sep 25 2009 SJT: Add idempotence header.
 * Jul 1  2009 SJT: Created.
 *
 */

#ifndef TV_DATA_DISPLAY_H_INCLUDED
#define TV_DATA_DISPLAY_H_INCLUDED 1

#define TV_TTF_DATA_DISPLAY_API_VERSION 1

#ifdef __cplusplus
extern "C" {
#endif

/* TV_ttf_display_type should return one of these values  */
enum TV_ttf_format_result
  {
    TV_ttf_format_ok,       /* Type is known, and successfully converted */
    TV_ttf_format_ok_elide, /* as TV_ttf_format_ok, but elide type       */
    TV_ttf_format_failed,   /* Type is known, but could not convert it */
    TV_ttf_format_raw,      /* Just display it as a regular type for now */
    TV_ttf_format_never     /* Don't know about this type, and please don't ask again */
  };
typedef enum TV_ttf_format_result TV_ttf_format_result;

/* TV_ttf_add_row returns one of these values */
enum TV_ttf_error_codes
  {
    TV_ttf_ec_ok  = 0,          /* operation succeeded                 */
    TV_ttf_ec_not_active,
    TV_ttf_ec_invalid_characters,
    TV_ttf_ec_buffer_exhausted
  };
typedef enum TV_ttf_error_codes TV_ttf_error_codes;

#define TV_ttf_type_ascii_string "$string"
#define TV_ttf_type_int "$int"
#if 0
#define TV_elide_row ""     /* field_name to use when row elision is desired */
#endif

/* returns logical true (non-zero) if the TV_ttf_format_result fr represents
   a format result that indicates success
*/
extern int TV_ttf_is_format_result_ok ( TV_ttf_format_result fr );

/* 
                  TV_ttf_ec_ok: Success
          TV_ttf_ec_not_active: Called with no active callback to
	                        TV_ttf_display_type
  TV_ttf_ec_invalid_characters: field_name or type_name has illegal characters
    TV_ttf_ec_buffer_exhausted: No more room left for display data
*/
extern int TV_ttf_add_row(const char *field_name,
                          const char *type_name,
                          const void *value);


#ifdef __cplusplus
}
#endif

#endif
