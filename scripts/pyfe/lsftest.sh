#! /usr/bin/bash
# LSF
# bsub -q pdebug -nnodes 4 -Is bash
# bkill -s KILL jobid
# set SCR_PKG to the directory where SCR is cloned
# set SCR_BUILD to the directory where SCR should be untarred and built (this will be removed with rm -rf)
# set SCR_INSTALL to the directory where SCR is installed
cd ../../../
export SCR_PKG=$(pwd)
#running script directly, need to add .py to scripts
#export PYFEBIN="python3 ${SCR_PKG}/scripts/pyfe/pyfe/"
export PYFEBIN="python3 -m pyfe."
export SCR_BUILD=${SCR_PKG}/build
export SCR_INSTALL=${SCR_PKG}/install

# cd to examples directory, and check that build of test programs works
#cd ${SCR_INSTALL}/share/scr/examples
#export OPT="-g -O0"
#make
cd ${SCR_BUILD}/examples

# run this from an interactive allocation of 4 nodes
# get an allocation
#mxterm 4 32 120
#salloc -N4 -ppdebug
#lalloc 4 -q pdebug

# set up initial enviroment for testing
export scrbin=${SCR_INSTALL}/bin
export jobid=`${PYFEBIN}scr_env --jobid`
echo "jobid = $jobid"
export nodelist=`${PYFEBIN}scr_env --nodes`
echo "nodelist = $nodelist"
export downnode=`${PYFEBIN}scr_glob_hosts -n 1 -h "$nodelist"`
echo "downnode = $downnode"
export prefix_files=".scr/flush.scr .scr/halt.scr .scr/nodes.scr"

#export LD_LIBRARY_PATH=${SCR_INSTALL}/lib:${SCR_PKG}/install/lib:/opt/ibm/spectrumcomputing/lsf/10.1/linux3.10-glibc2.17-ppc64le/lib
export LD_LIBRARY_PATH=${SCR_INSTALL}/lib:${LD_LIBRARY_PATH}
export SCR_PREFIX=`pwd`
export SCR_FETCH=0
export SCR_FLUSH=0
export SCR_DEBUG=1
export SCR_LOG_ENABLE=0
export SCR_JOB_NAME=testing_job
export SCR_CACHE_BYPASS=0
export SCR_CACHE_SIZE=2

# if there is a configuration file
#export SCR_CONF_FILE=~/myscr.conf

echo "clean out any cruft from previous runs"
echo "deletes files from cache and any halt, flush, nodes files"
lrun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
lrun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}


echo "check that a run works"
${PYFEBIN}scr_lrun -n4 -N4 ./test_api


echo "run again, check that checkpoints continue where last run left off"
${PYFEBIN}scr_lrun -n4 -N4 ./test_api


echo "delete all files from /ssd on rank 0, run again, check that rebuild works"
rm -rf /dev/shm/${USER}/scr.$jobid
rm -rf /ssd/${USER}/scr.$jobid
${PYFEBIN}scr_lrun -n4 -N4 ./test_api


echo "delete latest checkpoint directory from two nodes, run again,"
echo "check that rebuild works for older checkpoint"
lrun -n2 -N2 /bin/rm -rf /ssd/${USER}/scr.$jobid/scr.dataset.18
lrun -n2 -N2 /bin/rm -rf /dev/shm/${USER}/scr.$jobid/scr.dataset.18
${PYFEBIN}scr_lrun -n4 -N4 ./test_api


echo "delete all files from all nodes, run again, check that run starts over"
lrun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
lrun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
${PYFEBIN}scr_lrun -n4 -N4 ./test_api


echo "clear the cache and control directory"
lrun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
lrun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}


echo "check that scr_list_dir.py returns good values"
${PYFEBIN}scr_list_dir control
${PYFEBIN}scr_list_dir --base control
${PYFEBIN}scr_list_dir cache
${PYFEBIN}scr_list_dir --base cache


echo "check that scr_list_down_nodes.py returns good values"
#${PYFEBIN}scr_list_down_nodes
#${PYFEBIN}scr_list_down_nodes --down $downnode
#${PYFEBIN}scr_list_down_nodes --reason --down $downnode
export SCR_EXCLUDE_NODES=$downnode
#${PYFEBIN}scr_list_down_nodes
#${PYFEBIN}scr_list_down_nodes --reason
unset SCR_EXCLUDE_NODES


echo "check that scr_halt.py seems to work"
${PYFEBIN}scr_halt --list `pwd`; sleep 3
${PYFEBIN}scr_halt --before '3pm today' `pwd`; sleep 3
${PYFEBIN}scr_halt --after '4pm today' `pwd`; sleep 3
${PYFEBIN}scr_halt --seconds 1200 `pwd`; sleep 3
${PYFEBIN}scr_halt --unset-before `pwd`; sleep 3
${PYFEBIN}scr_halt --unset-after `pwd`; sleep 3
${PYFEBIN}scr_halt --unset-seconds `pwd`; sleep 3
${PYFEBIN}scr_halt `pwd`; sleep 3
${PYFEBIN}scr_halt --checkpoints 3 `pwd`; sleep 3
${PYFEBIN}scr_halt --unset-checkpoints `pwd`; sleep 3
${PYFEBIN}scr_halt --unset-reason `pwd`; sleep 3
${PYFEBIN}scr_halt --remove `pwd`


echo "check that scr_env.py seems to work"
${PYFEBIN}scr_env --user
${PYFEBIN}scr_env --jobid
${PYFEBIN}scr_env --nodes
${PYFEBIN}scr_env --down


echo "check that scr_prerun works"
${PYFEBIN}scr_prerun


echo "check that scr_postrun works (w/ empty cache)"
${PYFEBIN}scr_postrun


echo "clear the cache, make a new run, and check that scr_postrun scavenges successfully (no rebuild)"
lrun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
lrun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
${PYFEBIN}scr_lrun -n4 -N4 ./test_api
${PYFEBIN}scr_postrun
echo "scr_index (not a py script)"
${scrbin}/scr_index --list


echo "fake a down node via EXCLUDE_NODES and redo above test (check that rebuild during scavenge works)"
export SCR_EXCLUDE_NODES=$downnode
${PYFEBIN}scr_lrun -n4 -N4 ./test_api
${PYFEBIN}scr_postrun
unset SCR_EXCLUDE_NODES
${scrbin}/scr_index --list


echo "delete all files, enable fetch, run again, check that fetch succeeds"
lrun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
lrun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
export SCR_FETCH=1
${PYFEBIN}scr_lrun -n4 -N4 ./test_api
${scrbin}/scr_index --list


echo "delete all files from 2 nodes, run again, check that distribute fails but fetch succeeds"
lrun -n2 -N2 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
lrun -n2 -N2 /bin/rm -rf /ssd/${USER}/scr.$jobid
${PYFEBIN}scr_lrun -n4 -N4 ./test_api
${scrbin}/scr_index --list


# this test case is broken until we add CRC support back
## delete all files, corrupt file on disc, run again, check that fetch of current fails but old succeeds
#srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
#srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
##vi -b ${SCR_INSTALL}/share/scr/examples/scr.dataset.12/rank_2.ckpt
#sed -i 's/\?/i/' ${SCR_INSTALL}/share/scr/examples/scr.dataset.12/rank_2.ckpt
## change some characters and save file (:wq)
#srun -n4 -N4 ./test_api
#${scrbin}/scr_index --list


echo "enable flush, run again and check that flush succeeds and that postrun realizes that"
export SCR_FLUSH=10
${PYFEBIN}scr_lrun -n4 -N4 ./test_api
${PYFEBIN}scr_postrun
${scrbin}/scr_index --list

#echo "turn on the watchdog with a timeout of 15 seconds"
#export SCR_WATCHDOG=1
#export SCR_WATCHDOG_TIMEOUT=15
#export SCR_WATCHDOG_TIMEOUT_PFS=15
#echo "run test_api_hang, which will sleep for 90 seconds before finalizing"

#${PYFEBIN}scr_lrun -n4 -N4 ./test_api_hang
#echo "scr_lrun returned, try to restart (should get the checkpoint that was just made)"
#unset SCR_WATCHDOG
#unset SCR_WATCHDOG_TIMEOUT
#unset SCR_WATCHDOG_TIMEOUT_PFS
#${PYFEBIN}scr_lrun -n4 -N4 ./test_api
#echo "back again"

echo "----------------------"
echo "        srun          "
echo "----------------------"

# clear cache and check that scr_srun works
srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}
${PYFEBIN}scr_srun -n4 -N4 ./test_api
${scrbin}/scr_index --list

${PYFEBIN}scr_srun -n4 -N4 ./test_ckpt

${PYFEBIN}scr_srun -n4 -N4 ./test_config

echo "----------------------"
echo "       jsrun          "
echo "----------------------"
# clear cache and check that scr_srun works
jsrun --tasks_per_rs=1 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
jsrun --tasks_per_rs=1 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}
${PYFEBIN}scr_jsrun --tasks_per_rs=1  ./test_api
${scrbin}/scr_index --list

${PYFEBIN}scr_jsrun --tasks_per_rs=1 ./test_ckpt

${PYFEBIN}scr_jsrun --tasks_per_rs=1 ./test_config

echo "----------------------"
echo "        lrun          "
echo "----------------------"
# clear cache and check that scr_srun works
lrun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
lrun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}
${PYFEBIN}scr_lrun -n4 -N4 ./test_api
${scrbin}/scr_index --list

${PYFEBIN}scr_lrun -n4 -N4 ./test_ckpt

${PYFEBIN}scr_lrun -n4 -N4 ./test_config

echo "----------------------"
echo "      mpirun          "
echo "----------------------"
# clear cache and check that scr_srun works
mpirun -N 1 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
mpirun -N 1 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}
${PYFEBIN}scr_mpirun -N 1 ./test_api
${scrbin}/scr_index --list

${PYFEBIN}scr_mpirun -N 1 ./test_ckpt

${PYFEBIN}scr_mpirun -N 1 ./test_config

