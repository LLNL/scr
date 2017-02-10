#!/bin/bash
#this script expects:
# $1 to be path to scr build,
# $2 to be csv hostlist,
# $3 to be path to where the hostlist can be written
# $4 path to the node local output file for the test
# $5 path to the combined global output file for the test
source scr_base.sh
orterun -hnp file:$CAH_ORTE_HNP_FILE -x PATH -x LD_LIBRARY_PATH -x PMIX_NODELIST -x CNSS_PREFIX -np 1 `pwd`/job_script_simple.sh $1 $2 $3 $4 $5
