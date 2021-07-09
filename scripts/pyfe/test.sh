#! /usr/bin/bash
# run this from an interactive allocation of 4 nodes
#
# bsub -q pdebug -nnodes 4 -Is bash
# bkill -s KILL jobid
#
# lalloc 4 -q pdebug
#
# salloc -N4 -ppdebug
#

# Ensure the scripts are executable
./dochmod.sh

# Set the launcher for the launch script to use below
launcher="srun"
if [ $launcher == "srun" ]; then
  launcherargs="-n4 -N4"
elif [ $launcher == "lrun" ]; then
  launcherargs="-n4 -N4"
elif [ $launcher == "jsrun" ]; then
  launcherargs="--tasks_per_rs=1"
else
  launcher="mpirun"
  launcherargs="-N 1"
fi
################################ aprun?

export PATH=$(pwd)/pyfe:${PATH}
cd ../../../
export SCR_PKG=$(pwd)
export SCR_BUILD=${SCR_PKG}/build
export SCR_INSTALL=${SCR_PKG}/install

# cd to examples directory, and check that build of test programs works
#cd ${SCR_INSTALL}/share/scr/examples
#export OPT="-g -O0"
#make
cd ${SCR_BUILD}/examples

#export LD_LIBRARY_PATH=${SCR_INSTALL}/lib:${SCR_PKG}/install/lib:/opt/ibm/spectrumcomputing/lsf/10.1/linux3.10-glibc2.17-ppc64le/lib
export LD_LIBRARY_PATH=${SCR_INSTALL}/lib:${LD_LIBRARY_PATH}
export SCR_PREFIX=$(pwd)
export SCR_FETCH=0
export SCR_FLUSH=0
#export SCR_DEBUG=1
export SCR_LOG_ENABLE=0
export SCR_JOB_NAME=testing_job
export SCR_CACHE_BYPASS=0
export SCR_CACHE_SIZE=2

# if there is a configuration file
#export SCR_CONF_FILE=~/myscr.conf

# set up initial enviroment for testing
scrbin=${SCR_INSTALL}/bin
jobid=$(scr_env.py --jobid)
echo "jobid = ${jobid}"
nodelist=$(scr_env.py --nodes)
echo "nodelist = ${nodelist}"
downnode=$(scr_glob_hosts.py -n 1 -h "${nodelist}")
echo "downnode = ${downnode}"
prefix_files=".scr/flush.scr .scr/halt.scr .scr/nodes.scr"
sleep 1

echo "scr_const.py"
scr_const.py
sleep 1

echo "scr_common.py"
scr_common.py --interpolate .
scr_common.py --interpolate ../some_neighbor_directory
scr_common.py --interpolate "SCR_JOB_NAME = \$SCR_JOB_NAME"
scr_common.py --runproc echo -e 'this\nis\na\ntest'
scr_common.py --pipeproc echo -e 'this\nis\na\ntest' : grep t : grep e

sleep 2

echo "scr_env.py"
echo "The user: $(scr_env.py -u)"
echo "The jobid: $(scr_env.py -j)"
echo "The nodes: $(scr_env.py -n)"
echo "The downnodes: $(scr_env.py -d)"
echo "Runnode count (last run): $(scr_env.py -r)"
sleep 1

echo "scr_get_jobstep_id.py"
scr_get_jobstep_id.py
sleep 1

echo "scr_list_dir.py"
scr_list_dir.py control
scr_list_dir.py --base control
scr_list_dir.py cache
scr_list_dir.py --base cache
sleep 1

echo "scr_list_down_nodes.py"
scr_list_down_nodes.py -r ${nodelist}
sleep 1

echo "scr_param.py"
scr_param.py
sleep 1

echo "scr_prerun.py"
scr_prerun.py && echo "prerun passed" || echo "prerun failed"
sleep 1

echo "scr_test_runtime.py"
scr_test_runtime.py
sleep 1

echo "scr_run test_api . . ."
sleep 3

echo "clean out any cruft from previous runs"
echo "deletes files from cache and any halt, flush, nodes files"
${launcher} ${launcherargs} /bin/rm -rf /dev/shm/${USER}/scr.${jobid}
${launcher} ${launcherargs} /bin/rm -rf /ssd/${USER}/scr.${jobid}
rm -f ${prefix_files}

echo "check that a run works"
sleep 1
scr_${launcher}.py ${launcherargs} ./test_api

echo "run again, check that checkpoints continue where last run left off"
sleep 2
scr_${launcher}.py ${launcherargs} ./test_api

echo "delete all files from /ssd on rank 0, run again, check that rebuild works"
sleep 2
rm -rf /dev/shm/${USER}/scr.${jobid}
rm -rf /ssd/${USER}/scr.${jobid}
scr_${launcher}.py ${launcherargs} ./test_api

echo "delete all files from all nodes, run again, check that run starts over"
sleep 2
${launcher} ${launcherargs} /bin/rm -rf /ssd/${USER}/scr.${jobid}
${launcher} ${launcherargs} /bin/rm -rf /dev/shm/${USER}/scr.${jobid}
scr_${launcher}.py ${launcherargs} ./test_api


echo "clear the cache and control directory"
sleep 2
${launcher} ${launcherargs} /bin/rm -rf /dev/shm/${USER}/scr.${jobid}
${launcher} ${launcherargs} /bin/rm -rf /ssd/${USER}/scr.${jobid}
rm -f ${prefix_files}


echo "check that scr_list_dir.py returns good values"
sleep 1
scr_list_dir.py control
scr_list_dir.py --base control
scr_list_dir.py cache
scr_list_dir.py --base cache
sleep 1


echo "check that scr_list_down_nodes.py returns good values"
sleep 1
scr_list_down_nodes.py
scr_list_down_nodes.py --down ${downnode}
scr_list_down_nodes.py --reason --down ${downnode}
export SCR_EXCLUDE_NODES=${downnode}
scr_list_down_nodes.py
scr_list_down_nodes.py --reason
unset SCR_EXCLUDE_NODES
sleep 1


echo "check that scr_halt.py seems to work"
sleep 1
scr_halt.py --list $(pwd)
scr_halt.py --before '3pm today' $(pwd)
scr_halt.py --after '4pm today' $(pwd)
scr_halt.py --seconds 1200 $(pwd)
scr_halt.py --unset-before $(pwd)
scr_halt.py --unset-after $(pwd)
scr_halt.py --unset-seconds $(pwd)
scr_halt.py $(pwd)
scr_halt.py --checkpoints 3 $(pwd)
scr_halt.py --unset-checkpoints $(pwd)
scr_halt.py --unset-reason $(pwd)
scr_halt.py --remove $(pwd)
sleep 1


echo "check that scr_postrun works (w/ empty cache)"
sleep 1
scr_postrun.py


echo "clear the cache, make a new run"
sleep 2
${launcher} ${launcherargs} /bin/rm -rf /dev/shm/${USER}/scr.${jobid}
${launcher} ${launcherargs} /bin/rm -rf /ssd/${USER}/scr.${jobid}
scr_${launcher}.py ${launcherargs} ./test_api
sleep 1
echo "check that scr_postrun scavenges successfully (no rebuild)"
scr_postrun.py
sleep 1
echo "scr_index"
${scrbin}/scr_index --list
sleep 2

echo "fake a down node via EXCLUDE_NODES and redo above test (check that rebuild during scavenge works)"
sleep 1
export SCR_EXCLUDE_NODES=${downnode}
scr_${launcher}.py ${launcherargs} ./test_api
sleep 1
scr_postrun.py
sleep 1
unset SCR_EXCLUDE_NODES
${scrbin}/scr_index --list
sleep 2

echo "delete all files, enable fetch, run again, check that fetch succeeds"
sleep 1
${launcher} ${launcherargs} /bin/rm -rf /dev/shm/${USER}/scr.${jobid}
${launcher} ${launcherargs} /bin/rm -rf /ssd/${USER}/scr.${jobid}
export SCR_FETCH=1
scr_${launcher}.py ${launcherargs} ./test_api
sleep 1
${scrbin}/scr_index --list
sleep 2


# this test case is broken until we add CRC support back
## delete all files, corrupt file on disc, run again, check that fetch of current fails but old succeeds
#srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.${jobid}
#srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.${jobid}
##vi -b ${SCR_INSTALL}/share/scr/examples/scr.dataset.12/rank_2.ckpt
#sed -i 's/\?/i/' ${SCR_INSTALL}/share/scr/examples/scr.dataset.12/rank_2.ckpt
## change some characters and save file (:wq)
#srun -n4 -N4 ./test_api
#${scrbin}/scr_index --list


echo "enable flush, run again and check that flush succeeds and that postrun realizes that"
sleep 1
export SCR_FLUSH=10
scr_${launcher}.py ${launcherargs} ./test_api
sleep 1
scr_postrun.py
sleep 1
${scrbin}/scr_index --list
sleep 2

#echo "turn on the watchdog with a timeout of 15 seconds"
#export SCR_WATCHDOG=1
#export SCR_WATCHDOG_TIMEOUT=15
#export SCR_WATCHDOG_TIMEOUT_PFS=15
#echo "run test_api_hang, which will sleep for 90 seconds before finalizing"

#/scr_${launcher}.py -n4 -N4 ./test_api_hang
#echo "scr_${launcher} returned, try to restart (should get the checkpoint that was just made)"
#unset SCR_WATCHDOG
#unset SCR_WATCHDOG_TIMEOUT
#unset SCR_WATCHDOG_TIMEOUT_PFS
#/scr_${launcher}.py -n4 -N4 ./test_api
#echo "back again"

export SCR_DEBUG=0
echo "----------------------"
echo "        ${launcher}"
echo "----------------------"
sleep 2
# clear cache and check that scr_srun works
${launcher} ${launcherargs} /bin/rm -rf /dev/shm/${USER}/scr.${jobid}
${launcher} ${launcherargs} /bin/rm -rf /ssd/${USER}/scr.${jobid}
rm -f ${prefix_files}
scr_${launcher}.py ${launcherargs} ./test_api
${scrbin}/scr_index --list
sleep 2
scr_${launcher}.py ${launcherargs} ./test_ckpt
sleep 1
scr_${launcher}.py ${launcherargs} ./test_config
sleep 1

