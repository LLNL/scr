#! /usr/bin/bash
# run this from an interactive allocation of N nodes

### Set these variables ###
launcher="flux"
# Set number of nodes in allocation
numnodes="2"
# Set mpi C compiler for the sleeper/watchdog test
MPICC="mpicc"

export TESTDIR=$(pwd)
cd ..
export PATH=$(pwd)/pyfe:${PATH}
cd ../../../
export SCR_PKG=$(pwd)
export SCR_BUILD=${SCR_PKG}/build
export SCR_INSTALL=${SCR_PKG}/install

if [ $launcher == "srun" ]; then
  launcherargs="-n${numnodes} -N${numnodes}"
  singleargs="-n1 -N1"
elif [ $launcher == "lrun" ]; then
  launcherargs="-n${numnodes} -N${numnodes}"
  singleargs="-n1 -N1"
elif [ $launcher == "jsrun" ]; then
  launcherargs="--cpu_per_rs 1 --np ${numnodes}"
  singleargs="--cpu_per_rs 1 --np 1"
elif [ $launcher == "aprun" ]; then
  nodelist=$(scr_env.py --nodes)
  launcherargs="-L ${nodelist}"
  singleargs="-L ${nodelist}"
elif [ $launcher == "flux" ]; then
  launcherargs="--nodes=${numnodes} --ntasks=${numnodes} --cores-per-task=1"
  singleargs="--nodes=1 --ntasks=1 --cores-per-task=1"
else
  launcher="mpirun"
  launcherargs="-N 1"
  singleargs="-N 1"
fi

export LD_LIBRARY_PATH=${SCR_INSTALL}/lib:${LD_LIBRARY_PATH}
export SCR_FETCH=0
export SCR_DEBUG=1
export SCR_JOB_NAME=testing_job
export SCR_PREFIX=${SCR_BUILD}/examples

# Do tests
cd ${TESTDIR}

# Make the other test programs
${MPICC} -o sleeper sleeper.c
${MPICC} -o printer printer.c

if [ "$1" == "scripts" ] || [ "$1" == "" ]; then

  # Clear any leftover files
  rm -rf .scr/ ckpt.* out* cache/
  # Do a predefined run to give expected values
  echo ""
  echo "----------------------"
  echo ""
  echo "Running test_api with CACHE_BYPASS=0, CACHE_SIZE=6, FLUSH=6"
  echo ""
  echo "----------------------"
  echo ""
  export SCR_CACHE_BYPASS=0
  export SCR_CACHE_SIZE=6
  export SCR_FLUSH=6
  scr_${launcher}.py ${singleargs} ${SCR_BUILD}/examples/test_api --output 4
  sleep 1
  # Run any scripts in pyfe/tests/test*.py
  for testscript in ${TESTDIR}/test*.py; do
    echo ""
    echo "----------------------"
    echo ""
    echo "${testscript##*/}"
    sleep 1
    if [ "${testscript##*/}" == "test_watchdog.py" ]; then
      ${testscript} ${launcher} ${launcherargs} $(pwd)/sleeper
    elif [ "${testscript##*/}" == "test_launch.py" ]; then
      ${testscript} ${launcher} ${launcherargs} $(pwd)/printer
    elif [ "${testscript##*/}" == "test_pdsh.py" ]; then
      ${testscript} ${launcher} $(pwd)/printer
    else
      ${testscript}
    fi
    echo ""
    echo "----------------------"
    echo ""
    sleep 3
  done
  unset SCR_CACHE_BYPASS
  unset SCR_CACHE_SIZE
  unset SCR_FLUSH
fi

if [ "$1" == "scripts" ]; then
  exit 0
fi

if [ -x "sleeper" ]; then
  echo "Testing the watchdog"
  export SCR_WATCHDOG=1
  export SCR_WATCHDOG_TIMEOUT=1
  export SCR_WATCHDOG_TIMEOUT_PFS=1
  echo ""
  echo "Launching sleeper . . ."
  scr_${launcher}.py ${launcherargs} $(pwd)/sleeper
  unset SCR_WATCHDOG
  unset SCR_WATCHDOG_TIMEOUT
  unset SCR_WATCHDOG_TIMEOUT_PFS
  echo "Execution has returned, watchdog test concluded"
  sleep 3
fi

# cd to examples directory, and check that build of test programs works
#cd ${SCR_INSTALL}/share/scr/examples
#export OPT="-g -O0"
#make
cd ${SCR_BUILD}/examples
rm -rf .scr/

#export LD_LIBRARY_PATH=${SCR_INSTALL}/lib:${SCR_PKG}/install/lib:/opt/ibm/spectrumcomputing/lsf/10.1/linux3.10-glibc2.17-ppc64le/lib
export SCR_FLUSH=0
export SCR_LOG_ENABLE=0
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

echo ""
echo "scr_const.py"
scr_const.py
sleep 1

echo ""
echo "scr_common.py"
scr_common.py --interpolate .
scr_common.py --interpolate ../some_neighbor_directory
scr_common.py --interpolate "SCR_JOB_NAME = \$SCR_JOB_NAME"
scr_common.py --runproc echo -e 'this\nis\na\ntest'
scr_common.py --pipeproc echo -e 'this\nis\na\ntest' : grep t : grep e

sleep 2

echo ""
echo "scr_env.py"
echo "The user: $(scr_env.py -u)"
echo "The jobid: $(scr_env.py -j)"
echo "The nodes: $(scr_env.py -n)"
echo "The downnodes: $(scr_env.py -d)"
echo "Runnode count (last run): $(scr_env.py -r)"
sleep 1

echo ""
echo "scr_get_jobstep_id.py"
scr_get_jobstep_id.py ${launcher}
sleep 1

echo ""
echo "scr_list_dir.py"
scr_list_dir.py control
scr_list_dir.py --base control
scr_list_dir.py cache
scr_list_dir.py --base cache
sleep 2

echo ""
echo "scr_list_down_nodes.py"
scr_list_down_nodes.py -r ${nodelist}
sleep 1

echo ""
echo "scr_param.py"
scr_param.py
sleep 1

echo ""
echo "scr_prerun.py"
scr_prerun.py && echo "prerun passed" || echo "prerun failed"
sleep 2

echo ""
echo "scr_test_runtime.py"
scr_test_runtime.py
sleep 1

echo "scr_run test_api . . ."
sleep 2

echo ""
echo "clean out any cruft from previous runs"
echo "deletes files from cache and any halt, flush, nodes files"
rm -rf /dev/shm/${USER}/scr.${jobid}
rm -rf /ssd/${USER}/scr.${jobid}
rm -f ${prefix_files}

echo ""
echo "check that a run works"
sleep 1
scr_${launcher}.py ${launcherargs} ./test_api

echo ""
echo "run again, check that checkpoints continue where last run left off"
sleep 2
scr_${launcher}.py ${launcherargs} ./test_api

echo ""
echo "delete all files from /ssd on rank 0, run again, check that rebuild works"
sleep 2
rm -rf /dev/shm/${USER}/scr.${jobid}
rm -rf /ssd/${USER}/scr.${jobid}
scr_${launcher}.py ${launcherargs} ./test_api

echo ""
echo "delete all files from all nodes, run again, check that run starts over"
sleep 2
rm -rf /ssd/${USER}/scr.${jobid}
rm -rf /dev/shm/${USER}/scr.${jobid}
scr_${launcher}.py ${launcherargs} ./test_api

echo ""
echo "clear the cache and control directory"
sleep 2
rm -rf /dev/shm/${USER}/scr.${jobid}
rm -rf /ssd/${USER}/scr.${jobid}
rm -f ${prefix_files}

echo ""
echo "check that scr_list_dir.py returns good values"
sleep 2
scr_list_dir.py control
scr_list_dir.py --base control
scr_list_dir.py cache
scr_list_dir.py --base cache
sleep 2

echo ""
echo "check that scr_list_down_nodes.py returns good values"
sleep 1
scr_list_down_nodes.py
scr_list_down_nodes.py --down ${downnode}
scr_list_down_nodes.py --reason --down ${downnode}
export SCR_EXCLUDE_NODES=${downnode}
scr_list_down_nodes.py
scr_list_down_nodes.py --reason
unset SCR_EXCLUDE_NODES
sleep 2

echo ""
echo "check that scr_halt.py seems to work"
sleep 2
scr_halt.py --list $(pwd)
scr_halt.py --before '3pm today' $(pwd)
scr_halt.py --after '4pm today' $(pwd)
scr_halt.py --seconds 1200 $(pwd)
sleep 1
scr_halt.py --unset-before $(pwd)
scr_halt.py --unset-after $(pwd)
scr_halt.py --unset-seconds $(pwd)
scr_halt.py $(pwd)
sleep 1
scr_halt.py --checkpoints 3 $(pwd)
scr_halt.py --unset-checkpoints $(pwd)
scr_halt.py --unset-reason $(pwd)
scr_halt.py --remove $(pwd)
sleep 2

echo ""
echo "check that scr_postrun works (w/ empty cache)"
sleep 1
scr_postrun.py

echo ""
echo "clear the cache, make a new run"
sleep 2
rm -rf /dev/shm/${USER}/scr.${jobid}
rm -rf /ssd/${USER}/scr.${jobid}
scr_${launcher}.py ${launcherargs} ./test_api
sleep 1
echo "check that scr_postrun scavenges successfully (no rebuild)"
scr_postrun.py
sleep 2
echo "scr_index"
${scrbin}/scr_index --list
sleep 2

echo ""
echo "fake a down node via EXCLUDE_NODES and redo above test (check that rebuild during scavenge works)"
sleep 1
export SCR_EXCLUDE_NODES=${downnode}
scr_${launcher}.py ${launcherargs} ./test_api
sleep 3
echo ""
echo "scr_postrun.py"
scr_postrun.py
sleep 1
unset SCR_EXCLUDE_NODES
${scrbin}/scr_index --list
sleep 2

echo ""
echo "delete all files, enable fetch, run again, check that fetch succeeds"
sleep 1
rm -rf /dev/shm/${USER}/scr.${jobid}
rm -rf /ssd/${USER}/scr.${jobid}
export SCR_FETCH=1
scr_${launcher}.py ${launcherargs} ./test_api
sleep 2
echo "scr_index --list"
${scrbin}/scr_index --list
sleep 1

echo ""
echo "enable flush, run again and check that flush succeeds and that postrun realizes that"
sleep 2
export SCR_FLUSH=10
scr_${launcher}.py ${launcherargs} ./test_api
sleep 2
echo ""
echo "scr_postrun"
sleep 1
scr_postrun.py
sleep 2
echo ""
echo "scr_index --list"
sleep 1
${scrbin}/scr_index --list
echo "removing files . . ."
# clear cache and check that scr_srun works
rm -rf /dev/shm/${USER}/scr.${jobid}
rm -rf /ssd/${USER}/scr.${jobid}
rm -f ${prefix_files}
sleep 2

export SCR_DEBUG=0
echo ""
echo "----------------------"
echo "        ${launcher}"
echo "----------------------"
echo "running scr_${launcher}.py ${launcherargs} ./test_api"
sleep 1
scr_${launcher}.py ${launcherargs} ./test_api
sleep 2
echo ""
echo "running ${scrbin}/scr_index --list"
sleep 1
${scrbin}/scr_index --list
sleep 2
echo ""
echo "running scr${launcher}.py ${launcherargs} ./test_ckpt"
sleep 1
scr_${launcher}.py ${launcherargs} ./test_ckpt
sleep 2
echo ""
echo "running scr_${launcher}.py ${launcherargs} ./test_config"
sleep 1
scr_${launcher}.py ${launcherargs} ./test_config
rm -rf /dev/shm/${USER}/scr.${jobid}
rm -rf /ssd/${USER}/scr.${jobid}
rm -f ${prefix_files}
