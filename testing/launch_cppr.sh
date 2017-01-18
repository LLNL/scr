#!/bin/bash
set -x
#this script expects:
# $1 to be path to scr build,
# $2 to be csv hostlist,
# $3 to be path to where the hostlist can be written
source scr_base.sh
rm -f $3cppr-*.log
$CPPR_INST_PATH/TESTING/scripts/run_cppr.sh -l 4 -w $CPPR_CAH_WORKDIR -H $CAH_HOSTLIST -g $3 start


