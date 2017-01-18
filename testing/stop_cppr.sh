#!/bin/bash
set -x
#this script expects:
# $1 to be path to scr build,
# $2 to be csv hostlist,
# $3 to be path to where the hostlist can be written
source scr_base.sh

$CPPR_INST_PATH/TESTING/scripts/run_cppr.sh -l 5 -w $CPPR_CAH_WORKDIR -g $3 stop

sleep 2

