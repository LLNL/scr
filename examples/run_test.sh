#!/bin/bash

if [  "$1" != "" ]; then
    launch=$1
    shift
fi

if [  "$1" != "" ]; then
    test=$1
    shift
fi

if [ "$1" != "" ]; then
    restart=$1
    shift
fi

echo "Run: $launch $test $@"
$launch $test "$@"
RC=$?

# only attempt a restart if requested and if first run succeeded
if [ "$restart" = "restart" -a $RC -eq 0 ]; then
    echo "Restart: $launch $test $@"
    $launch $test "$@"
    RC=$?
fi

# delete files from compute nodes
$launch ./test_cleanup.sh

# delete files in prefix directory
rm -rf .scr/

exit $RC
