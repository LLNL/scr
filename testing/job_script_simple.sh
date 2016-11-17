#!/bin/bash
#this script expects:
# $1 to be path to scr build,
# $2 to be csv hostlist,
# $3 to be path to where the hostlist can be written
# $4 path to the node local output file for the test
# $5 path to the combined global output file for the test
source scr_base.sh
#orte-dvm --prefix $SL_OMPI_PREFIX --report-uri $3scr-orte-uri.txt -host $3scr_hostlist.txt
export SCR_PREFIX=$3
export SCR_CLUSTER_NAME=wolf
export SCR_JOB_NAME=scr_bat
export SCR_USER_NAME=$USER
#remove the previous test output files
rm -f $5
pdsh -w $2 rm -rf $4
which scr_pmix_run_bash >> $5
`pwd`/PMIX_TEST $1 $2 $4 $5
date >> $5
