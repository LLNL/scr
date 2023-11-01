#!/bin/bash
#BSUB -env "all, LSB_DJOB_COMMFAIL_ACTION=KILL_TASKS"

# This demonstrates how to integrate SCR into a batch job script.
#
# One should inform LSF to not kill the allocation on node failure.
#
# One needs to inform the SCR commands about the SCR_PREFIX directory.
# The commands look in that directory for files written by the SCR library.
#
# One must call scr_prerun to prepare the allocation for SCR.
#
# The script potentially runs multiple times in the allocation.
# Between runs, it uses scr_list_down_nodes to detect down nodes,
# and it excludes those nodes when launching using srun --exclude.
# It calls scr_should_exit to determine whether to exit the run loop.
#
# The run loop exits if:
#   - the run finishes normally (SCR_Finalize was called)
#   - the allocation has run out of sufficient nodes
#   - any other halt condition has been set
#   - the max run limit has been hit
#
# One should call scr_postrun to scavenge any datasets from cache.

# path to SCR install /bin directory
scrbin="@X_BINDIR@"

# NOTE: set this to your SCR_PREFIX directory
scr_prefix=`pwd`

# prepare allocation for SCR
${scrbin}/scr_prerun -p $scr_prefix
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
    # flag to tell jsrun to exclude_hosts any down nodes when launching
    exclude=""
    if [ "$down_nodes" != "" ] ; then
        exclude="--exclude_hosts $down_nodes"
    fi

    # launch the job, excluding any down nodes
    jsrun $exclude "$@"

    # check whether we should stop running
    ${scrbin}/scr_should_exit -p $scr_prefix
    if [ $? == 0 ] ; then
        echo "Halt condition detected, ending run."
        break
    fi

    # any retry attempts left?
    runs=$(($runs - 1))
    if [ $runs -le 0 ] ; then
        echo "Runs exhausted, ending run."
        break
    fi

    # give nodes a chance to clean up
    # NOTE: need to sleep longer than an internal timeout in jsrun
    sleep 120

    # check for down nodes
    # once a node is marked as down, we leave it as down
    keep_down=""
    if [ "$down_nodes" != "" ] ; then
        keep_down="--down $down_nodes"
    fi
    down_nodes=`${scrbin}/scr_list_down_nodes $keep_down`

    # in case of new down nodes, check whether we should stop again
    keep_down=""
    if [ "$down_nodes" != "" ] ; then
        keep_down="--down $down_nodes"
    fi
    ${scrbin}/scr_should_exit -p $scr_prefix $keep_down
    if [ $? == 0 ] ; then
        echo "Halt condition detected, ending run."
        break
    fi
done

# scavenge files from cache to prefix directory
${scrbin}/scr_postrun -p $scr_prefix -j jsrun
