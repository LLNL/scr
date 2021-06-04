#!/bin/bash

if [  "$1" != "" ]; then
    test=$1
    shift
fi

if [ "$1" != "" ]; then
    restart=$1
    shift
fi

echo "Running $test $@ restart=$restart"

$test "$@"
RC=$?

if [ "$restart" = "restart" -a $RC -eq 0 ]; then
    echo "Restarting"
    $test "$@"
    RC=$?
fi

./test_cleanup.sh

exit $RC
