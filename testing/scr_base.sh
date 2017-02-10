#!/bin/bash
#this script expects:
# $1 to be path to scr build,
# $2 to be csv hostlist,
# $3 to be path to where the hostlist can be written
set -e
set -x
CPPR_INST_PATH=`cat $1path.txt`
SCR_INST_PATH=$1scr_cppr
#SCR_INST_PATH=$1
CAH_HOSTLIST=$2

#these hardcoded values need to change for autotest
CPPR_CAH_WORKDIR=/tmp/automation/cppr
CAH_ORTE_HNP_FILE=$3scr-orte-uri.txt
CAH_HOSTLIST_FILE=$3scr_hostlist.txt

export SCR_INSTALL=$SCR_INST_PATH
source $1.build_vars.sh
#export MCL_LOG_LEVEL=5
#export MCL_LOG_DIR=/tmp/caholgu1
export PDSH_RCMD_TYPE=ssh
export CNSS_PREFIX=$CPPR_CAH_WORKDIR/local
export LD_LIBRARY_PATH=$CPPR_INST_PATH/lib:$SL_OMPI_PREFIX/../pmix/lib:$SCR_INST_PATH/lib:$SL_LD_LIBRARY_PATH
export PATH=$CPPR_INST_PATH/bin:$SL_PATH:$SCR_INST_PATH/bin:~/perl5/bin:/usr/lib64/qt-3.3/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin

export PMIX_NODELIST=$CAH_HOSTLIST
