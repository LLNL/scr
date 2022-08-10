#!/bin/bash

# Test if running within a job allocation
# by detecting if the jobid env var is set

if [[ $SLURM_JOBID ]]; then
    exit 0
elif [[ $LSB_JOBID ]]; then
    exit 0
fi

echo "ERROR: Not inside a job allocation. Force tests to run with:"
echo "'ctest' or 'ctest --verbose'"
exit 1
