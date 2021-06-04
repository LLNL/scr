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

$launch ./test_cleanup.sh

exit $RC
