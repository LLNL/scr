#!/bin/bash
#
# AXL metadata copy test.
#
# Make two files, set some metadata on them (size, times, permission), copy
# the file, and verify the metadata is correct.

src=$(mktemp -d)
dest=$(mktemp -d)

trap ctrl_c INT

function cleanup
{
	rm -fr "$src" "$dest"
}

function ctrl_c() {
	cleanup
}

# Create two files and set some metadata
dd if=/dev/zero of=$src/file1 bs=1 count=1
dd if=/dev/zero of=$src/file2 bs=1 count=5

chmod 444 $src/file1
chmod 777 $src/file2

if [ "$(uname)" == "Darwin" ] ; then
    TOUCH_CMD=gtouch
else
    TOUCH_CMD=touch
fi
$TOUCH_CMD -d "1 hour ago" $src/file1
$TOUCH_CMD -d "1 day ago" $src/file2

# Do a simple transfer and verify the result
./axl_cp -a $src/* $dest
src_ls="$(ls -l $src)"
dest_ls="$(ls -l $dest)"

if [ "$src_ls" != "$dest_ls" ] ; then
	echo "Error: source and dest metadata doesn't match:"
	echo "$src_ls"
	echo "-----------------------------------"
	echo "$dest_ls"
	cleanup
	exit 1
fi

rm -f $dest/*

# Now do another transfer without copying metadata. The permissions
# should be different between the source and destination.
./axl_cp $src/* $dest
src_ls="$(ls -l $src)"
dest_ls="$(ls -l $dest)"
if [ "$src_ls" == "$dest_ls" ] ; then
	echo "Error: source and dest metadata matches, but shouldn't:"
	echo "$src_ls"
	echo "-----------------------------------"
	echo "$dest_ls"
	cleanup
	exit 1
fi
cleanup
