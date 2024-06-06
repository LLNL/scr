#! /usr/bin/bash
# run this from an interactive allocation of N nodes

### Set these variables ###
launcher="srun"
numnodes="2"  # Number of nodes in allocation
MPICC="mpicc" # MPI compiler for sleeper/watchdog test
### Set these variables ###

if [ $launcher == "srun" ]; then
  launcherargs="-n${numnodes} -N${numnodes}"
  singleargs="-n1 -N1"
elif [ $launcher == "lrun" ]; then
  launcherargs="-n${numnodes} -N${numnodes}"
  singleargs="-n1 -N1"
elif [ $launcher == "jsrun" ]; then
  launcherargs="-n ${numnodes} -c 1"
  singleargs="-n 1 -c 1"
elif [ $launcher == "aprun" ]; then
  nodelist=$(python3 scr_env --nodes)
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

export TESTDIR=$(pwd)
#cd ..
#export PATH=$(pwd)/scrjob:${PATH}
cd ../../../
export SCR_PKG=$(pwd)
export SCR_BUILD=${SCR_PKG}/build
export SCR_INSTALL=${SCR_PKG}/install
export LD_LIBRARY_PATH=${SCR_INSTALL}/lib:${LD_LIBRARY_PATH}

scrbin=${SCR_INSTALL}/bin
scrlibexec=${SCR_INSTALL}/libexec/python
scriptdir=${scrlibexec}/scrjob

#export PATH=${scrbin}:${PATH}

export SCR_FETCH=1
export SCR_DEBUG=1
export SCR_JOB_NAME=testing_job
export SCR_PREFIX=${TESTDIR}
export SCR_CNTL_BASE=${TESTDIR}/cntl
export SCR_CACHE_BASE=${TESTDIR}/cache

export SCR_CACHE_BYPASS=0
export SCR_CACHE_SIZE=6
export SCR_FLUSH=6
export SCR_WATCHDOG=1
export SCR_WATCHDOG_TIMEOUT=20
export SCR_WATCHDOG_TIMEOUT_PFS=20
export SCR_LOG_ENABLE=1

delfiles="out* .scr/ ckpt* cache/ cntl/"

# Do tests
cd ${TESTDIR}
cp ${SCR_BUILD}/examples/{test_api,test_config,test_ckpt} .

# Make the other test programs
${MPICC} -o sleeper sleeper.c
${MPICC} -o printer printer.c

if [ "$1" == "scripts" ] || [ "$1" == "" ]; then

  # Clear any leftover files
  rm -rf ${delfiles}
  # Do a predefined run to give expected values
  echo ""
  echo "----------------------"
  echo ""
  echo "Running test_api with CACHE_BYPASS=0, CACHE_SIZE=6, FLUSH=6"
  echo ""
  echo "----------------------"
  echo ""
  ${launcher} ${singleargs} test_api --output 4
  sleep 1
  # Run any scripts in scrpy/tests/test*.py
  for testscript in ${TESTDIR}/test*.py; do
    echo ""
    echo "----------------------"
    echo ""
    echo "${testscript##*/}"
    sleep 1
    if [ "${testscript##*/}" == "test_watchdog.py" ]; then
      PYTHONPATH=${scrlibexec} python3 ${testscript} ${launcher} ${launcherargs} $(pwd)/sleeper
    elif [ "${testscript##*/}" == "test_launch.py" ]; then
      PYTHONPATH=${scrlibexec} python3 ${testscript} ${launcher} ${launcherargs} $(pwd)/printer
    elif [ "${testscript##*/}" == "test_pdsh.py" ]; then
      PYTHONPATH=${scrlibexec} python3 ${testscript} ${launcher} $(pwd)/printer
    elif [ "${testscript##*/}" == "test_flush_file.py" ]; then
      PYTHONPATH=${scrlibexec} python3 ${testscript} ${SCR_PREFIX}
    else
      PYTHONPATH=${scrlibexec} python3 ${testscript}
    fi
    echo ""
    echo "----------------------"
    echo ""
    sleep 3
  done
fi

if [ "$1" == "scripts" ]; then
  exit 0
fi

# can't change env variables within the flux
if [ $launcher != "flux" ] && [ -x "sleeper" ]; then
  echo "Testing the watchdog"
  export SCR_WATCHDOG=1
  export SCR_WATCHDOG_TIMEOUT=1
  export SCR_WATCHDOG_TIMEOUT_PFS=1
  echo ""
  echo "Launching sleeper . . ."
  ${scrbin}/scr_run ${launcher} ${launcherargs} $(pwd)/sleeper
  unset SCR_WATCHDOG
  unset SCR_WATCHDOG_TIMEOUT
  unset SCR_WATCHDOG_TIMEOUT_PFS
  echo "Execution has returned, watchdog test concluded"
  sleep 3
fi

rm -rf ${delfiles}

# vars for testing
jobid=$(python3 ${scrlibexec}/scr_env.py --jobid)
echo "jobid = ${jobid}"
nodelist=$(python3 ${scrlibexec}/scr_env.py --nodes)
echo "nodelist = ${nodelist}"
downnode=$(/usr/bin/hostlist -n 1 "${nodelist}")
echo "first node = ${downnode}"
sleep 2

echo ""
echo "config.py"
python3 ${scriptdir}/config.py
sleep 2

echo ""
echo "common.py"
PYTHONPATH=${scrlibexec} python3 ${scriptdir}/common.py --interpolate .
PYTHONPATH=${scrlibexec} python3 ${scriptdir}/common.py --interpolate ../some_neighbor_directory
PYTHONPATH=${scrlibexec} python3 ${scriptdir}/common.py --interpolate "SCR_JOB_NAME = \$SCR_JOB_NAME"
PYTHONPATH=${scrlibexec} python3 ${scriptdir}/common.py --runproc echo -e 'this\nis a\ntest'
PYTHONPATH=${scrlibexec} python3 ${scriptdir}/common.py --pipeproc echo -e 'this\nis a\ntest' : grep t : grep e

sleep 2

echo ""
echo "scr_env.py"
echo "The user: $(python3 ${scrlibexec}/scr_env.py -u)"
echo "The jobid: $(python3 ${scrlibexec}/scr_env.py -j)"
echo "The nodes: $(python3 ${scrlibexec}/scr_env.py -n)"
echo "The downnodes: $(python3 ${scrlibexec}/scr_env.py -d)"
echo "Runnode count (last run): $(python3 ${scrlibexec}/scr_env.py -r)"
sleep 2

echo ""
echo "scr_list_dir.py"
python3 ${scrlibexec}/scr_list_dir.py --prefix ${SCR_PREFIX} control
python3 ${scrlibexec}/scr_list_dir.py --prefix ${SCR_PREFIX} --base control
python3 ${scrlibexec}/scr_list_dir.py --prefix ${SCR_PREFIX} cache
python3 ${scrlibexec}/scr_list_dir.py --prefix ${SCR_PREFIX} --base cache
sleep 2

echo ""
echo "scr_list_down_nodes.py"
python3 ${scrbin}/scr_list_down_nodes --joblauncher ${launcher} -r ${nodelist}
sleep 1

echo ""
echo "param.py"
PYTHONPATH=${scrlibexec} python3 ${scriptdir}/param.py
sleep 2

echo ""
echo "scr_prerun.py"
python3 ${scrbin}/scr_prerun && echo "prerun passed" || echo "prerun failed"
sleep 2

echo ""
echo "test_runtime.py"
PYTHONPATH=${scrlibexec} python3 ${scriptdir}/test_runtime.py
sleep 1

echo "scr_run test_api . . ."
sleep 2

echo ""
echo "clean out any cruft from previous runs"
echo "deletes files from cache and any halt, flush, nodes files"
rm -rf ${delfiles}

echo ""
echo "check that a run works"
sleep 1
${launcher} ${launcherargs} ./test_api

echo ""
echo "run again, check that checkpoints continue where last run left off"
sleep 2
${launcher} ${launcherargs} ./test_api

echo ""
echo "delete all files from /ssd on rank 0, run again, check that rebuild works"
sleep 2
rm -rf ${delfiles}
${launcher} ${launcherargs} ./test_api

echo ""
echo "delete all files from all nodes, run again, check that run starts over"
sleep 2
rm -rf ${delfiles}
${launcher} ${launcherargs} ./test_api

echo ""
echo "clear the cache and control directory"
sleep 2
rm -rf ${delfiles}

echo ""
echo "check that scr_list_dir.py returns good values"
sleep 2
python3 ${scrlibexec}/scr_list_dir.py --prefix ${SCR_PREFIX} control
python3 ${scrlibexec}/scr_list_dir.py --prefix ${SCR_PREFIX} --base control
python3 ${scrlibexec}/scr_list_dir.py --prefix ${SCR_PREFIX} cache
python3 ${scrlibexec}/scr_list_dir.py --prefix ${SCR_PREFIX} --base cache
sleep 2

if [ $launcher != "flux" ]; then
  echo ""
  echo "check that scr_list_down_nodes.py returns good values"
  sleep 1
  ${scrbin}/scr_list_down_nodes --joblauncher ${launcher}
  ${scrbin}/scr_list_down_nodes --joblauncher ${launcher} --down ${downnode}
  ${scrbin}/scr_list_down_nodes --joblauncher ${launcher} --reason --down ${downnode}
  export SCR_EXCLUDE_NODES=${downnode}
  ${scrbin}/scr_list_down_nodes --joblauncher ${launcher}
  ${scrbin}/scr_list_down_nodes --joblauncher ${launcher} --reason
  unset SCR_EXCLUDE_NODES
  sleep 2
fi

echo ""
echo "check that scr_halt.py seems to work"
sleep 2
${scrbin}/scr_halt --list $(pwd)
${scrbin}/scr_halt --before '2100-07-04T21:00:00' $(pwd)
${scrbin}/scr_halt --after '2100-07-04T21:00:00' $(pwd)
${scrbin}/scr_halt --seconds 1200 $(pwd)
sleep 1
${scrbin}/scr_halt --unset-before $(pwd)
${scrbin}/scr_halt --unset-after $(pwd)
${scrbin}/scr_halt --unset-seconds $(pwd)
${scrbin}/scr_halt $(pwd)
sleep 1
${scrbin}/scr_halt --checkpoints 3 $(pwd)
${scrbin}/scr_halt --unset-checkpoints $(pwd)
${scrbin}/scr_halt --unset-reason $(pwd)
${scrbin}/scr_halt --remove $(pwd)
sleep 2

echo ""
echo "check that scr_postrun works (w/ empty cache)"
sleep 1
${scrbin}/scr_postrun --prefix ${SCR_PREFIX} --joblauncher ${launcher}

echo ""
echo "clear the cache, make a new run"
sleep 2
rm -rf ${delfiles}
${launcher} ${launcherargs} ./test_api
sleep 1
echo "check that scr_postrun scavenges successfully (no rebuild)"
${scrbin}/scr_postrun --prefix ${SCR_PREFIX} --joblauncher ${launcher}
sleep 2
echo "scr_index"
${scrbin}/scr_index --list
sleep 2

if [ $launcher != "flux" ]; then
  echo ""
  echo "fake a down node via EXCLUDE_NODES and redo above test (check that rebuild during scavenge works)"
  sleep 1
  export SCR_EXCLUDE_NODES=${downnode}
  ${launcher} ${launcherargs} ./test_api
  sleep 3
  echo ""
  echo "scr_postrun"
  ${scrbin}/scr_postrun --prefix ${SCR_PREFIX} --joblauncher ${launcher}
  sleep 1
  unset SCR_EXCLUDE_NODES
  ${scrbin}/scr_index --list
  sleep 2
fi

echo ""
echo "delete all files, run again, check that fetch succeeds"
sleep 1
rm -rf ${delfiles}
${launcher} ${launcherargs} ./test_api
sleep 2
echo "scr_index --list"
${scrbin}/scr_index --list
sleep 1

echo "removing files . . ."
# clear cache and check that scr_srun works
rm -rf ${delfiles}
sleep 2

echo ""
echo "----------------------"
echo "        ${launcher}"
echo "----------------------"
echo "running scr_${launcher}.py ${launcherargs} ./test_api"
sleep 1
${scrbin}/scr_${launcher} ${launcherargs} ./test_api
sleep 2
echo ""
echo "running ${scrbin}/scr_index --list"
sleep 1
${scrbin}/scr_index --list
sleep 2
echo ""
echo "running scr${launcher}.py ${launcherargs} ./test_ckpt"
sleep 1
${scrbin}/scr_${launcher} ${launcherargs} ./test_ckpt
sleep 2
echo ""
echo "running scr_${launcher}.py ${launcherargs} ./test_config"
sleep 1
${scrbin}/scr_${launcher} ${launcherargs} ./test_config
rm -rf ${delfiles}
