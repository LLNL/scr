#!/bin/bash
set -x
#this script expects:
# $1 to be path to scr build,
# $2 to be csv hostlist,
# $3 to be path to where the hostlist can be written
source scr_base.sh

orte-dvm --prefix $SL_OMPI_PREFIX --report-uri $CAH_ORTE_HNP_FILE -hostfile $CAH_HOSTLIST_FILE & MY_PID=$!;
echo $MY_PID > $3scrpidfile
