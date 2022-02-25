#! /bin/bash

# keep these to reset path / ld path
export FLUXTESTSVPATH=${PATH}
export FLUXTESTSVLDPATH=${LD_LIBRARY_PATH}

export TESTDIR=$(pwd)
cd ..
export PATH=$(pwd)/scrjob:${PATH}
cd ../../../
export SCR_PKG=$(pwd)
export SCR_BUILD=${SCR_PKG}/build
export SCR_INSTALL=${SCR_PKG}/install
export LD_LIBRARY_PATH=${SCR_INSTALL}/lib:${LD_LIBRARY_PATH}
export SCR_FETCH=1
export SCR_DEBUG=1
export SCR_JOB_NAME=testing_job

cd ${TESTDIR}

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
