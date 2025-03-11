/* 
 * $Header: /home/tv/src/debugger/src/datadisp/tv_data_display.c,v 1.5 2010/10/04 04:02:19 anb Exp $
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
 * Sep 27 2010 ANB: lots of changes as part of totalview/12314.
 *                  Reworked to reduce the dependencies on outside
 *                  entities, both at compile and also at runtime.
 *                  Adjusted the naming scheme.
 * Jan 28 2010 SJT: Bug 12100, bump base size to 16K and recognize if it is
 *                  resized further.
 * Sep 24 2009 SJT: Remove pre/post callback to reduce function call overhead.
 * Jul 1  2009 SJT: Created.
 *
 */

#ifdef __cplusplus
extern "C" {
#endif

#include "tv_data_display.h"

#include <stdio.h>
#include <stddef.h>             /* for size_t */

#define DATA_FORMAT_BUFFER_SIZE 16384
#define TV_FORMAT_INACTIVE 0
#define TV_FORMAT_FIRST_CALL 1
#define TV_FORMAT_APPEND_CALL 2

volatile int TV_ttf_data_format_control      = TV_FORMAT_INACTIVE;
int          TV_ttf_data_display_api_version = TV_TTF_DATA_DISPLAY_API_VERSION;
   
/* TV_ttf_data_format_buffer should not be static for icc 11, and others */
char TV_ttf_data_format_buffer[DATA_FORMAT_BUFFER_SIZE];
static char *TV_ttf_data_buffer_ptr = TV_ttf_data_format_buffer;

static const char   digits []  = "0123456789abcdefghijklmnopqrstuvwxyz";
static const size_t base_bound = sizeof ( digits );

/* ************************************************************************ */

int
TV_ttf_is_format_result_ok ( TV_ttf_format_result fr )
{
  int  ret_val;

  switch ( fr )
    {
    case TV_ttf_format_ok:
    case TV_ttf_format_ok_elide:
      ret_val = 1;
      break;
    default:
      ret_val = 0;
      break;
    }
  return ret_val;
} /* TV_ttf_is_format_result_ok */

/* ************************************************************************ */

static
void *
my_zeroit ( void *s, size_t n )
{
  char *cp = (char *) s;

  /* not the most efficient of solutions.  What we should do is   */
  /* do the assugnments in units of int or long.  The problem     */
  /* with that is ensuring that the alignments of the assignments */
  /* are correct.  The difficulty with that is doing arithmetic   */
  /* on pointers in a portable manner.                            */
  while ( n > 0 )
    {
      *cp++ = 0;
      n--;
    }

  return s;
} /* my_zeroit */

static
char *
my_strpbrk ( const char *str, const char *accept )
{
  char  *ret_val = NULL;
  char  *s, *t;

  for ( s = (char *) str; (*s) && (! ret_val); s++ )
    {
      for ( t = (char *) accept; (*t) && (! ret_val); t++ )
        {
          if ( *s == *t )
            ret_val = s;
        }
    }

  return ret_val;
} /* my_strpbrk */

static
int
marshal_string ( char *buffer, size_t len, const char *s,
                                                 char **nbuffer, size_t *nlen )
{
  int   ret_val = 0;
  char *cursor  = buffer;

  while ( *s )
    {
      ret_val++;
      if ( len > 1 )
        {
          *cursor++ = *s++;
          len--;
        }
    }
  if ( len > 0 )
    *cursor = '\0';

  if ( nbuffer )
    *nbuffer = cursor;
  if ( nlen )
    *nlen = len;

  return ret_val;
} /* marshal_string */

static
int
marshal_unsigned_body ( char *buffer, size_t len, size_t val, int base,
                                                char **nbuffer, size_t *nlen )
{

  int     ret_val = 0;
  size_t  q, r;
  char    digit [ 2 ];
  char   *my_buffer  = buffer;
  size_t  my_len     = len;

  if ( val < base )
    {
      r = val;
    }
  else
    {
      q        = val / base;
      r        = val - (q * base);
      ret_val += marshal_unsigned_body ( buffer, len, q, base,
                                                     &my_buffer, &my_len );
    }
  digit [ 0 ]  = digits [ r ];
  digit [ 1 ]  = '\0';
  ret_val     += marshal_string ( my_buffer, my_len, digit, nbuffer, nlen );

  return ret_val;
} /* marshal_unsigned_body */

static
int
marshal_unsigned ( char *buffer, size_t len, size_t val, int base,
                                                char **nbuffer, size_t *nlen )
{
  int     ret_val = 0;

  if ( 0 == base )
    base = 10;
  if ( base < base_bound )
    ret_val = marshal_unsigned_body ( buffer, len, val, base, nbuffer, nlen );
  else
    ret_val = -1;

  return ret_val;
} /* marshal_unsigned */

static
int
marshal_hex ( char *buffer, size_t len, size_t hex_val,
                                              char **nbuffer, size_t *nlen  )
{
  int     ret_val = 0;
  char   *my_buffer;
  size_t  my_len;

  ret_val += marshal_string ( buffer, len, "0x", &my_buffer, &my_len );
  ret_val += marshal_unsigned ( my_buffer, my_len, hex_val, 16, nbuffer, nlen );

  return ret_val;
} /* marshal_hex */

static
int
marshal_row ( char *buffer, size_t len, const char   *field_name,
                                        const char   *type_name,
                                        const void   *value,
                                        char        **nbuffer,
                                        size_t       *nlen )
{
  int     ret_val = 0;
  char   *my_buffer;
  size_t  my_len;

  ret_val += marshal_string ( buffer, len, field_name, &my_buffer, &my_len );
  ret_val += marshal_string ( my_buffer, my_len, "\t", &my_buffer, &my_len );
  ret_val += marshal_string ( my_buffer, my_len, type_name, &my_buffer, &my_len );
  ret_val += marshal_string ( my_buffer, my_len, "\t", &my_buffer, &my_len );
  ret_val += marshal_hex ( my_buffer, my_len, (size_t) value, &my_buffer, &my_len );
  ret_val += marshal_string ( my_buffer, my_len, "\n", nbuffer, nlen );

  return ret_val;
} /* marshal_row */

int TV_ttf_add_row(const char *field_name,
                   const char *type_name,
                   const void *value)
{
  size_t remaining;
  int out;

  /*
  printf ( "TV_ttf_add_row: on entry TV_ttf_data_format_control == %d\n", TV_ttf_data_format_control );
  */

  /* Called at the wrong time */
  if (TV_ttf_data_format_control == TV_FORMAT_INACTIVE)
    return TV_ttf_ec_not_active;
    
  if (my_strpbrk(field_name, "\n\t") != NULL)
    return TV_ttf_ec_invalid_characters;

  if (my_strpbrk(type_name, "\n\t") != NULL)
    return TV_ttf_ec_invalid_characters;

  if (TV_ttf_data_format_control == TV_FORMAT_FIRST_CALL)
    {
      /* Zero out the buffer to avoid confusion, and set the write point 
         to the top of the buffer. */

      my_zeroit(TV_ttf_data_format_buffer, sizeof (TV_ttf_data_format_buffer));
      TV_ttf_data_buffer_ptr     = TV_ttf_data_format_buffer;
      TV_ttf_data_format_control = TV_FORMAT_APPEND_CALL;
    }
        
  remaining = TV_ttf_data_buffer_ptr +
              DATA_FORMAT_BUFFER_SIZE - TV_ttf_data_format_buffer;
  
/*
  out = snprintf(TV_ttf_data_buffer_ptr, 
                 remaining, "%s\t%s\t%p\n", 
                 field_name, type_name, value);
*/
  out = marshal_row ( TV_ttf_data_buffer_ptr, remaining,
                      field_name, type_name, value, 0, 0 );
  
  if (out < 1)
    return TV_ttf_ec_buffer_exhausted;
    
  TV_ttf_data_buffer_ptr += out;
  
  return 0;
} /* TV_ttf_add_row */

void TV_ttf_pre_display_callback(void)
{
  TV_ttf_data_format_control = TV_FORMAT_FIRST_CALL;
}

void TV_ttf_post_display_callback(void)
{
  TV_ttf_data_format_control = TV_FORMAT_INACTIVE;
}

#ifdef __cplusplus
}
#endif
