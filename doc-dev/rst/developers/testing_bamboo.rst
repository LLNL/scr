.. _test_bamboo:

Bamboo Test Suite
=================

SCR's bamboo testing suite does a few things:

1. Test that SCR will build with CMake
2. Trigger SCR's built-in testing with `make test` (uses ctest)
3. Test that SCR will build with Spack
4. Trigger more advanced test by running the `testing/TEST` script

This document records some of the scripts for the bamboo test suite.
Within bamboo, the test plans have the ability to specify different
machines which on which a particular step can be run. In this way, SCR
can be tested for each platform.

Bamboo Test Plan Overview
-------------------------

1. Clone SCR from the repository and check out the particular branch
   which is being tested. This functionality is built-in to bamboo
   and does not have an associated script.
2. **Build and Make**::

     #!/bin/bash

     . /etc/profile
     . /etc/bashrc

     mkdir build install
     cd build
     cmake -DCMAKE_INSTALL_PREFIX=../install ../SCR
     make
     make install

3. **Test parallel**::

     cd build
     ctest --verbose -T Test -R parallel*
     mkdir Testing/Tests
     cp `grep -rl '.xml$' Testing/*` Testing/*/* Testing/Tests

4. **Test serial**::

     cd build
     ctest --verbose -T Test -R serial*

5. Bamboo has a built-in CTest test parser. This is configured with the
   test file path pattern: `**/Testing/*/*.xml`
6. Clone spack from its repository.
7. **Install SCR**::

     #!/bin/bash -l
     . /etc/profile
     . /etc/bashrc

     cd spack
     sed -i "s#/.spack#/.spack-${SYS_TYPE}#" lib/spack/spack/__init__.py
     . share/spack/setup-env.sh
     #spack compiler find

     spack install --keep-stage scr@develop resource_manager=SLURM
     #spack install --run-tests scr@develop resource_manager=NONE

     spack cd -i scr
     cd share/scr/examples
     export MPICC=/usr/tce/packages/mvapich2/mvapich2-2.2-gcc-4.9.3/bin/mpicc
     export MPICXX=/usr/tce/packages/mvapich2/mvapich2-2.2-gcc-4.9.3/bin/mpicxx
     export MPIF77=/usr/tce/packages/mvapich2/mvapich2-2.2-gcc-4.9.3/bin/mpif77
     export MPIF90=/usr/tce/packages/mvapich2/mvapich2-2.2-gcc-4.9.3/bin/mpif90
     make

8. **Run SCR/Testing/TEST python script**::

     #!/bin/bash -l
     # This script takes 1 variable, the script you want to run.
     # This variable, $1, comes from the bamboo command line.
     # Here, $1 = TEST

     . spack/share/spack/setup-env.sh

     # setup environment for script to be run

     spack cd scr@develop
     export SCR_PKG=`pwd`

     spack cd -i scr@develop
     export SCR_INSTALL=`pwd`

     cp $SCR_PKG/testing/$1 $SCR_INSTALL/bin/$1

     cd $SCR_INSTALL/bin
     export SCR_LAUNCH_DIR=`pwd`

     # submit job
     jobID=$(sbatch --export=ALL -ppbatch -n4 -N4 -t5 -J SCR-TESTS -o bamboo_test_$1.out $1 | tr -dc [:digit:])

     # watch and wait until job has completed
     # this no longer works because bamboo has its own timeout
     #jobInfo=$(mdiag -j ${jobID})
     #jobStatus=$(echo $jobInfo | awk '{print $13}')

     #while [ $jobStatus != "CG" ] && [ $jobStatus != "CD" ]
     #do
     #    jobInfo=$(mdiag -j ${jobID})
     #    jobStatus=$(echo $jobInfo | awk '{print $13}')
     #done

     # watch and wait until job has completed
     jobStatus=$(checkjob ${jobID} | grep State | awk '{print $2}')

     count=1
     while [ "$jobStatus" != "Completed" ]; do
     jobStatus=$(checkjob ${jobID} | grep State | awk '{print $2}')
     if ([ "$jobStatus" = "Idle" ] || [ "$jobStatus" = "Resources" ]) && [ $((count % 60)) -eq 0 ]; then
     echo "Job $jobID waiting for resources"
     count=1
     fi
     ((count++))
     sleep 1
     done

     checkjob ${jobID}

     # print results of script
     if [ -e bamboo_test_$1.out ]; then
     cat bamboo_test_$1.out
     else
     echo "File bamboo_test_$1.out does not exist"
     exit 1
     fi

     # determine if script was successful
     result=$(cat bamboo_test_$1.out | tail -n5 | grep -o PASSED)

     # post test cleanup
     rm -rf .scr/ ckpt.*

     if [ "$result" != "PASSED" ]; then
     exit 1
     fi

     exit 0
