#!/bin/bash

if [  "$1" != "" ]; then
    launch=$1
    shift
fi

if [  "$1" != "" ]; then
    launch_args=$1
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

echo "Run: $launch $launch_args $test $@"
$launch $launch_args $test "$@"
RC=$?

# only attempt a restart if requested and if first run succeeded
if [ "$restart" = "restart" -a $RC -eq 0 ]; then
    echo "Restart: $launch $launch_args $test $@"
    $launch $launch_args $test "$@"
    RC=$?
fi

# delete files from compute nodes
$launch ./test_cleanup.sh

# delete files in prefix directory

# In the hope of working better with NFS, lets rename the directory
# first and then remove the newly named file.  This should (hopefully)
# allow subsequent tests not to fail when/if compute nodes have stale
# references to the old prefix directory.
#
mv .scr .scr.remove.me/
rm -rf .scr.remove.me/

exit $RC
