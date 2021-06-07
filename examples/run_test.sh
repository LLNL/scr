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

echo "Running $launch $test $@ restart=$restart"

$launch $test "$@"
RC=$?

if [ "$restart" = "restart" -a $RC -eq 0 ]; then
    echo "Restarting"
    $launch $test "$@"
    RC=$?
fi

# delete files from compute nodes
$launch ./test_cleanup.sh

# delete files in prefix directory
rm -rf .scr/

exit $RC
