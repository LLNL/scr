#!/bin/bash
set -x

# this is only needed when dependencies are built separately from SCR's bootstrap.sh
topdir=`pwd`/..
export LD_LIBRARY_PATH=$topdir/scr-dist/install/lib64:$topdir/deps/install/lib:$topdir/deps/install/lib64:$LD_LIBRARY_PATH

which python3

# delete any state from previous runs
rm -rf .scr ckpt*

# check that a sequence of runs follows the correct order of checkpoints
srun -n 2 python scr_test.py 0
srun -n 2 python scr_test.py 1
srun -n 2 python scr_test.py 2

# including one where the app indicates that a process failed to read its checkpoint file
srun -n 2 python scr_test.py 3

# test that calling scr.complete_output before start_output aborts
# the C library aborts here, so there is no clean way to catch this in python
srun -n 2 python scr_test.py 4

# check that scr.init throws an exception if SCR_Init returns an error
srun -n 2 python scr_test.py 5
