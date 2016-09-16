#!/bin/bash

export CFLAGS="-g -O0"
export configopts="--with-scr-config-file=/etc/scr.conf --with-yogrt --with-mysql"
export scrversion="scr-1.1.8"

make distclean
./autogen.sh
./configure --prefix=/usr/local/tools/scr-1.1 $configopts
make dist
make distcheck
