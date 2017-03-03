#!/bin/sh
echo "Running aclocal ... "
aclocal -I config -I m4
echo "Running libtoolize ... (use glibtoolize on a Mac)"
libtoolize --automake --copy
echo "Running autoheader ... "
autoheader
echo "Running automake ... "
automake --copy --add-missing
echo "Running autoconf ... "
autoconf
echo "Cleaning up ..."
mv aclocal.m4 config/
rm -rf autom4te.cache

echo "Now run ./configure."
