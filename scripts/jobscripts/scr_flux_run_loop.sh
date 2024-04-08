#!/bin/bash
# FLUX: -oexit-timeout=none

# This demonstrates how to integrate SCR into a batch job script.
#
# One should inform Flux not to kill the allocation if one of the Flux
# brokers crashes (this is the effect of `-oexit-timeout=none`)
#
# One needs to inform the SCR commands about the SCR_PREFIX directory.
# The commands look in that directory for files written by the SCR library.
#
# One must call scr_prerun to prepare the allocation for SCR.
#
# The script potentially runs multiple times in the allocation.
# Between runs, it uses scr_list_down_nodes to detect down nodes,
# and it excludes those nodes when launching using flux --exclude.
# It calls scr_should_exit to determine whether to exit the run loop.
#
# The run loop exits if one of the following holds:
#   - the run finishes normally (SCR_Finalize was called)
#   - the allocation has run out of sufficient nodes
#   - any other halt condition has been set
#   - the max run limit has been hit
#
# One should call scr_postrun to scavenge any datasets from cache.

# for debugging
set -x
verbose="-v"

# path to SCR install /bin directory
scrbin="@X_BINDIR@"

# NOTE: set this to your SCR_PREFIX directory
scr_prefix=$(pwd)

# prepare allocation for SCR
${scrbin}/scr_prerun -p $scr_prefix $verbose
if [ $? -ne 0 ] ; then
    echo "ERROR: scr_prerun -p $scr_prefix"
    exit 1
fi

# stores list of nodes detected to be down
down_nodes=""

# max number of runs to attempt
runs=5

# enter the run loop
while [ 1 ] ; do
    # flag to tell flux to exclude any down nodes when launching
    exclude=""
    if [ "$down_nodes" != "" ] ; then
        exclude="--requires=-host:$down_nodes"
    fi

    # launch the job, excluding any down nodes
    flux run $exclude "$@"

    # reduce number of remaining run attempts
    runs=$(($runs - 1))

    # check whether we should stop running
    ${scrbin}/scr_should_exit -p $scr_prefix --runs $runs $verbose
    if [ $? == 0 ] ; then
        echo "Halt condition detected, ending run."
        break
    fi

    # give nodes a chance to clean up
    sleep 60

    # check for down nodes
    # once a node is marked as down, we leave it as down
    keep_down=""
    if [ "$down_nodes" != "" ] ; then
        keep_down="--down $down_nodes"
    fi
    down_nodes=$(${scrbin}/scr_list_down_nodes -j flux $keep_down)

    # in case of new down nodes, check whether we should stop again
    keep_down=""
    if [ "$down_nodes" != "" ] ; then
        keep_down="--down $down_nodes"
    fi
    ${scrbin}/scr_should_exit -p $scr_prefix $keep_down $verbose
    if [ $? == 0 ] ; then
        echo "Halt condition detected, ending run."
        break
    fi
done

# scavenge files from cache to prefix directory
${scrbin}/scr_postrun -p $scr_prefix -j flux $verbose
