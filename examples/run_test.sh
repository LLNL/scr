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

if [ "$restart" = "restart" ]; then
    echo "Restarting"
    $test "$@"
fi

./test_cleanup.sh

