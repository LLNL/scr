#!/bin/bash
#SBATCH --no-kill

# This demonstrates how to integrate SCR into a batch job script.
#
# One should inform sbatch to not kill the allocation on node failure.
# 
# One needs to inform the SCR commands about the SCR_PREFIX directory.
# The commands look in that directory for files written by the SCR library.
#
# One must call scr_prerun to prepare the allocation for SCR.
# One should call scr_postrun to scavenge any datasets from cache.

# TODO: set this to the SCR_PREFIX directory
scr_prefix=`pwd`

# path to SCR install /bin directory
bindir="@X_BINDIR@"

# prepare allocation for SCR
$bindir/scr_prerun -p $scr_prefix
if [ $? -ne 0 ] ; then
  echo "ERROR: scr_prerun -p $scr_prefix"
  exit 1
fi

# launch the job
srun "$@"

# scavenge files from cache to prefix directory
$bindir/scr_postrun -p $scr_prefix -j srun
