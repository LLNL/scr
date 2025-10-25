#!/bin/bash
#
#  Test runner script meant to be executed inside of a docker container
#
#  Usage: checks_run.sh [OPTIONS...]
#
#  Where OPTIONS are passed directly to `cmake``
#
#  The script is otherwise influenced by the following environment variables:
#
#  JOBS=N        Argument for make's -j option, default=2
#  PROJECT       Project name
#  CPPCHECK      Run cppcheck if set to "t"
#  RECHECK       Run `make recheck` if `make check` fails the first time
#  UNIT_TEST_ONLY Only run `make check` under ./src
#  QUICK_CHECK   Run only `make TESTS=` and a simple test
#
#  And, obviously, some crucial variables that configure itself cares about:
#
#  CC, CXX, LDFLAGS, CFLAGS, etc.
#

# source check_group and check_time functions:
. src/test/checks-lib.sh

ARGS="$@"
JOBS=${JOBS:-2}
MAKECMDS="make -j ${JOBS} install"
CHECKCMDS="ctest --output-on-failure"


# Force git to update the shallow clone and include tags so git-describe works
checks_group "git fetch tags" "git fetch --unshallow --tags" \
 git fetch --unshallow --tags || true

checks_group_start "build setup"
ulimit -c unlimited

source /etc/profile.d/modules.sh
module load mpi

if mpirun -V | grep "Open MPI"; then
  # without this flag, in GitHub Action Runners, mpirun will complain that there
  # aren't enough resources
  ARGS="-DMPIRUN_FLAGS='--map-by :OVERSUBSCRIBE' ${ARGS}"
fi


POSTCHECKCMDS=":"
# Enable coverage for $CC-coverage build
# We can't use distcheck here, it doesn't play well with coverage testing:

if test -n "$UNIT_TEST_ONLY"; then
  CHECKCMDS="(cd src && $CHECKCMDS)"
fi

checks_group_end # Setup

WORKDIR=$(pwd)
if test -n "$BUILD_DIR" ; then
  mkdir -p "$BUILD_DIR"
  cd "$BUILD_DIR"
  rm -f CMakeCache.txt
fi

checks_group "cmake ${ARGS}"  cmake ${ARGS} \
	|| checks_die "cmake failed" cat config.log
checks_group "make clean..." make clean

checks_group "${MAKECMDS}" "${MAKECMDS}" \
  || checks_die "${MAKECMDS} failed"

checks_group "${CHECKCMDS}" "${CHECKCMDS}" && \
	checks_group "${POSTCHECKCMDS}" "${POSTCHECKCMDS}"
RC=$?

if test "$RECHECK" = "t" -a $RC -ne 0; then
  #
  # `make recheck` is not recursive, only perform it if at least some tests
  #   under ./t were run (and presumably failed)
  #
  printf "::warning::make check failed, trying recheck\n"
  ${CHECKCMDS} --rerun-failed && \
    checks_group "${POSTCHECKCMDS}" "${POSTCHECKCMDS}"
  RC=$?
fi

exit $RC
