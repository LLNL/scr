/* Macros to swap the order of bytes in integer values.
   Copyright (C) 1997, 1998, 2000, 2002, 2003, 2007, 2008
   Free Software Foundation, Inc.
   This file is part of the GNU C Library.

   The GNU C Library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   The GNU C Library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with the GNU C Library; if not, write to the Free
   Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
   02111-1307 USA.  */

//#if !defined _BYTESWAP_H && !defined _NETINET_IN_H && !defined _ENDIAN_H
//# error "Never use <bits/byteswap.h> directly; include <byteswap.h> instead."
//#endif

#ifndef _SCR_BITS_BYTESWAP_H
#define _SCR_BITS_BYTESWAP_H 1

#include <bits/wordsize.h>

/* Swap bytes in 16 bit value.  */
#define __bswap_constant_16(x) \
     ((((x) >> 8) & 0xff) | (((x) & 0xff) << 8))

/* This is better than nothing.  */
# define scr_bswap_16(x) \
     (__extension__							      \
      ({ register unsigned short int __x = (x); __bswap_constant_16 (__x); }))


/* Swap bytes in 32 bit value.  */
#define __bswap_constant_32(x) \
     ((((x) & 0xff000000) >> 24) | (((x) & 0x00ff0000) >>  8) |		      \
      (((x) & 0x0000ff00) <<  8) | (((x) & 0x000000ff) << 24))

#  define scr_bswap_32(x)							      \
     (__extension__							      \
      ({ register unsigned int __x = (x); __bswap_constant_32 (__x); }))


/* Swap bytes in 64 bit value.  */
# define __bswap_constant_64(x) \
     ((((x) & 0xff00000000000000ull) >> 56)				      \
      | (((x) & 0x00ff000000000000ull) >> 40)				      \
      | (((x) & 0x0000ff0000000000ull) >> 24)				      \
      | (((x) & 0x000000ff00000000ull) >> 8)				      \
      | (((x) & 0x00000000ff000000ull) << 8)				      \
      | (((x) & 0x0000000000ff0000ull) << 24)				      \
      | (((x) & 0x000000000000ff00ull) << 40)				      \
      | (((x) & 0x00000000000000ffull) << 56))

# if __WORDSIZE == 64
#  define scr_bswap_64(x) \
     (__extension__                                                           \
      ({ union { __extension__ unsigned long long int __ll;                   \
                 unsigned int __l[2]; } __w, __r;                             \
           {                                                                  \
             __w.__ll = (x);                                                  \
             __r.__l[0] = __bswap_32 (__w.__l[1]);                            \
             __r.__l[1] = __bswap_32 (__w.__l[0]);                            \
           }                                                                  \
         __r.__ll; }))
#endif

#endif /* _BITS_BYTESWAP_H */
