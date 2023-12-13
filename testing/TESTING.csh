# set SCR_PKG to the directory where SCR is cloned
# set SCR_BUILD to the directory where SCR should be untarred and built (this will be removed with rm -rf)
# set SCR_INSTALL to the directory where SCR is installed
setenv SCR_PKG     `pwd`
setenv SCR_BUILD   `pwd`/scr-dist
setenv SCR_INSTALL "${SCR_BUILD}/install"

# CORAL build instructions
cd ${SCR_PKG}
rm -rf ${SCR_BUILD}
mkdir ${SCR_BUILD}
cd ${SCR_BUILD}
setenv CFLAGS "-g -O0"
setenv depsinstalldir ${SCR_PKG}/install
cmake \
  -DCMAKE_INSTALL_PREFIX=${SCR_INSTALL} \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_VERBOSE_MAKEFILE=true \
  -DSCR_RESOURCE_MANAGER=LSF \
  -DWITH_DTCMP_PREFIX=$depsinstalldir \
  -DWITH_SPATH_PREFIX=$depsinstalldir \
  -DWITH_KVTREE_PREFIX=$depsinstalldir \
  -DWITH_AXL_PREFIX=$depsinstalldir \
  -DWITH_RANKSTR_PREFIX=$depsinstalldir \
  -DWITH_REDSET_PREFIX=$depsinstalldir \
  -DWITH_SHUFFILE_PREFIX=$depsinstalldir \
  -DWITH_ER_PREFIX=$depsinstalldir \
  ${SCR_PKG}
make
make install

# Linux cmake build instructions
cd ${SCR_PKG}
rm -rf ${SCR_BUILD}
mkdir ${SCR_BUILD}
cd ${SCR_BUILD}
setenv CFLAGS "-g -O0"
setenv depsinstalldir ${SCR_PKG}/install
cmake \
  -DCMAKE_INSTALL_PREFIX=${SCR_INSTALL} \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_VERBOSE_MAKEFILE=true \
  -DWITH_DTCMP_PREFIX=$depsinstalldir \
  -DWITH_SPATH_PREFIX=$depsinstalldir \
  -DWITH_KVTREE_PREFIX=$depsinstalldir \
  -DWITH_AXL_PREFIX=$depsinstalldir \
  -DWITH_RANKSTR_PREFIX=$depsinstalldir \
  -DWITH_REDSET_PREFIX=$depsinstalldir \
  -DWITH_SHUFFILE_PREFIX=$depsinstalldir \
  -DWITH_ER_PREFIX=$depsinstalldir \
  ${SCR_PKG}
make
make install

# cd to examples directory, and check that build of test programs works
#cd ${SCR_INSTALL}/share/scr/examples
#setenv OPT "-g -O0"
#make
cd ${SCR_BUILD}/examples

#cp ~/myscr.conf .scrconf

# get an allocation
#mxterm 4 32 120
salloc -N4 -ppdebug

# set up initial enviroment for testing
setenv scrbin ${SCR_INSTALL}/bin
setenv jobid `${scrbin}/scr_env --jobid`
echo "$jobid"
setenv nodelist `${scrbin}/scr_env --nodes`
echo "$nodelist"
setenv downnode `${scrbin}/scr_glob_hosts -n 1 -h "$nodelist"`
echo "$downnode"
setenv prefix_files ".scr/flush.scr .scr/halt.scr .scr/nodes.scr"

setenv LD_LIBRARY_PATH ${SCR_INSTALL}/lib64:${SCR_PKG}/install/lib
setenv SCR_PREFIX `pwd`
setenv SCR_FETCH 0
setenv SCR_FLUSH 0
setenv SCR_DEBUG 1
setenv SCR_LOG_ENABLE 0
setenv SCR_JOB_NAME testing_job
setenv SCR_CACHE_BYPASS 0
setenv SCR_CACHE_SIZE 2

#setenv SCR_CONF_FILE ~/myscr.conf

# clean out any cruft from previous runs
# deletes files from cache and any halt, flush, nodes files
srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}

# check that a run works
srun -n4 -N4 ./test_api

# run again, check that checkpoints continue where last run left off
srun -n4 -N4 ./test_api

# delete all files from /ssd on rank 0, run again, check that rebuild works
rm -rf /dev/shm/${USER}/scr.$jobid
rm -rf /ssd/${USER}/scr.$jobid
srun -n4 -N4 ./test_api

# delete latest checkpoint directory from two nodes, run again,
# check that rebuild works for older checkpoint
srun -n2 -N2 /bin/rm -rf /ssd/${USER}/scr.$jobid/scr.dataset.18
srun -n2 -N2 /bin/rm -rf /dev/shm/${USER}/scr.$jobid/scr.dataset.18
srun -n4 -N4 ./test_api

# delete all files from all nodes, run again, check that run starts over
srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
srun -n4 -N4 ./test_api

# clear the cache and control directory
srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}

# check that scr_list_dir returns good values
${scrbin}/scr_list_dir control
${scrbin}/scr_list_dir --base control
${scrbin}/scr_list_dir cache
${scrbin}/scr_list_dir --base cache

# check that scr_list_down_nodes returns good values
${scrbin}/scr_list_down_nodes
${scrbin}/scr_list_down_nodes --down $downnode
${scrbin}/scr_list_down_nodes --reason --down $downnode
setenv SCR_EXCLUDE_NODES $downnode
${scrbin}/scr_list_down_nodes
${scrbin}/scr_list_down_nodes --reason
unsetenv SCR_EXCLUDE_NODES

# check that scr_halt seems to work
${scrbin}/scr_halt --list `pwd`; sleep 5
${scrbin}/scr_halt --before '2100-07-04T21:00:00' `pwd`; sleep 5
${scrbin}/scr_halt --after '2100-07-04T21:00:00' `pwd`; sleep 5
${scrbin}/scr_halt --seconds 1200 `pwd`; sleep 5
${scrbin}/scr_halt --unset-before `pwd`; sleep 5
${scrbin}/scr_halt --unset-after `pwd`; sleep 5
${scrbin}/scr_halt --unset-seconds `pwd`; sleep 5
${scrbin}/scr_halt `pwd`; sleep 5
${scrbin}/scr_halt --checkpoints 5 `pwd`; sleep 5
${scrbin}/scr_halt --unset-checkpoints `pwd`; sleep 5
${scrbin}/scr_halt --unset-reason `pwd`; sleep 5
${scrbin}/scr_halt --remove `pwd`

# check that scr_env seems to work
${scrbin}/scr_env --user
${scrbin}/scr_env --jobid
${scrbin}/scr_env --nodes
${scrbin}/scr_env --down

# check that scr_prerun works
${scrbin}/scr_prerun

# check that scr_postrun works (w/ empty cache)
${scrbin}/scr_postrun

# clear the cache, make a new run, and check that scr_postrun scavenges successfully (no rebuild)
srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
srun -n4 -N4 ./test_api
${scrbin}/scr_postrun
${scrbin}/scr_index --list

# fake a down node via EXCLUDE_NODES and redo above test (check that rebuild during scavenge works)
setenv SCR_EXCLUDE_NODES $downnode
srun -n4 -N4 ./test_api
${scrbin}/scr_postrun
unsetenv SCR_EXCLUDE_NODES
${scrbin}/scr_index --list

# delete all files, enable fetch, run again, check that fetch succeeds
srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
setenv SCR_FETCH 1
srun -n4 -N4 ./test_api
${scrbin}/scr_index --list

# delete all files from 2 nodes, run again, check that distribute fails but fetch succeeds
srun -n2 -N2 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
srun -n2 -N2 /bin/rm -rf /ssd/${USER}/scr.$jobid
srun -n4 -N4 ./test_api
${scrbin}/scr_index --list

# this test case is broken until we add CRC support back
## delete all files, corrupt file on disc, run again, check that fetch of current fails but old succeeds
#srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
#srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
#vi -b ${SCR_INSTALL}/share/scr/examples/ckpt.12/rank_2.ckpt
## change some characters and save file (:wq)
#srun -n4 -N4 ./test_api
#${scrbin}/scr_index --list

#enable flush, run again and check that flush succeeds and that postrun realizes that
setenv SCR_FLUSH 10
srun -n4 -N4 ./test_api
${scrbin}/scr_postrun
${scrbin}/scr_index --list

# clear cache and check that scr_srun works
srun -n4 -N4 /bin/rm -rf /dev/shm/${USER}/scr.$jobid
srun -n4 -N4 /bin/rm -rf /ssd/${USER}/scr.$jobid
rm -f ${prefix_files}
${scrbin}/scr_srun -n4 -N4 ./test_api
${scrbin}/scr_index --list
