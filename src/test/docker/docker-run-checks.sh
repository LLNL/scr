#!/bin/bash
#
#  Build flux "checks" docker image and run tests, exporting
#   important environment variables to the docker environment.
#
#  Arguments here are passed directly to ./configure
#
#
# option Defaults:
PROJECT=scr

WORKDIR=/usr/src
IMAGE=el9
JOBS=2
MOUNT_HOME_ARGS="--volume=$HOME:$HOME -e HOME"
BUILD_DIR=build

#
declare -r prog=${0##*/}
die() { echo -e "$prog: $@"; exit 1; }

#
declare -r long_opts="help,quiet,interactive,image:,jobs:,no-cache,no-home,tag:,build-directory:,install-only,recheck,unit-test-only,quick-check,platform:,workdir:,system"
declare -r short_opts="hqIi:S:j:t:D:Prup:"
declare usage="
Usage: $prog [OPTIONS] -- [CMAKE_ARGS...]\n\
Build docker image for CI builds, then run tests inside the new\n\
container as the current user and group.\n\
\n\
Uses the current git repo for the build.\n\
\n\
Options:\n\
 -h, --help                    Display this message\n\
     --no-cache                Disable docker caching\n\
     --no-home                 Skip mounting the host home directory\n\
     --install-only            Skip make check, only make install\n\
 -q, --quiet                   Add --quiet to docker-build\n\
 -t, --tag=TAG                 If checks succeed, tag image as NAME\n\
 -i, --image=NAME              Use base docker image NAME (default=$IMAGE)\n\
 -p, --platform=NAME           Run on alternate platform (if supported)\n\
 -j, --jobs=N                  Value for make -j (default=$JOBS)\n
 -r, --recheck                 Run 'make recheck' after failure\n\
 -u, --unit-test-only          Only run unit tests\n\
     --quick-check             Only run 'make check TESTS=' and one basic test\n\
 -D, --build-directory=DIRNAME Name of a subdir to build in, will be made\n\
     --workdir=PATH            Use PATH as working directory for build\n\
 -I, --interactive             Instead of running ci build, run docker\n\
                                image with interactive shell.\n\
"

# check if running in OSX
if [[ "$(uname)" == "Darwin" ]] && [[ $FORCE_GNU_GETOPT != 1 ]]; then
    # BSD getopt
    GETOPTS=`getopt $short_opts -- $*`
    usage=${usage}"\n\
    You are using BSD getopt on macOS. BSD getopt does not recognize '='\n\
    between options. Use a space instead. If gnu-getopt is first in your\n\
    PATH, force the script to use that by setting FORCE_GNU_GETOPT=1.\n"
else
    # GNU getopt
    GETOPTS=`getopt -u -o $short_opts -l $long_opts -n $prog -- $@`
    if [[ $? != 0 ]]; then
        die "$usage"
    fi
    eval set -- "$GETOPTS"
fi
while true; do
    case "$1" in
      -h|--help)                   echo -ne "$usage";          exit 0  ;;
      -q|--quiet)                  QUIET="--quiet";            shift   ;;
      -i|--image)                  IMAGE="$2";                 shift 2 ;;
      -p|--platform)               PLATFORM="--platform=$2";   shift 2 ;;
      -j|--jobs)                   JOBS="$2";                  shift 2 ;;
      -I|--interactive)            INTERACTIVE="/bin/bash";    shift   ;;
      -r|--recheck)                RECHECK=t;                  shift   ;;
      -u|--unit-test-only)         UNIT_TEST_ONLY=t;           shift   ;;
      --quick-check)               QUICK_CHECK=t;              shift   ;;
      -D|--build-directory)        BUILD_DIR="$2";             shift 2 ;;
      --build-arg)                 BUILD_ARG=" --build-arg $2" shift 2 ;;
      --workdir)                   WORKDIR="$2";               shift 2 ;;
      --no-cache)                  NO_CACHE="--no-cache";      shift   ;;
      --no-home)                   MOUNT_HOME_ARGS="";         shift   ;;
      --install-only)              INSTALL_ONLY=t;             shift   ;;
      -t|--tag)                    TAG="$2";                   shift 2 ;;
      --)                          shift; break;                       ;;
      *)                           die "Invalid option '$1'\n$usage"   ;;
    esac
done

TOP=$(git rev-parse --show-toplevel 2>&1) \
    || die "not inside $PROJECT git repository!"
which docker >/dev/null \
    || die "unable to find a docker binary"
if docker buildx >/dev/null 2>&1; then
    DOCKER_BUILD="docker buildx build --load"
else
    DOCKER_BUILD="docker build"
fi
DOCKER_BUILD="docker build"

CMAKE_ARGS="-DSCR_RESOURCE_MANAGER=NONE -DCMAKE_INSTALL_PREFIX=../install -DCMAKE_BUILD_TYPE=Debug $@ .."

. ${TOP}/src/test/checks-lib.sh

BUILD_IMAGE=${PROJECT}-checks-builder:${IMAGE}
DOCKERFILE=$TOP/src/test/docker/$IMAGE

checks_group "Building image $IMAGE for user $USER $(id -u) group=$(id -g)" \
  ${DOCKER_BUILD} \
    ${PLATFORM} \
    ${NO_CACHE} \
    ${QUIET} \
    --build-arg USER=$USER \
    --build-arg UID=$(id -u) \
    --build-arg GID=$(id -g) \
    ${BUILD_ARG:- } \
    -t ${BUILD_IMAGE} \
    -f ${DOCKERFILE}/Dockerfile \
    . || die "docker build failed"

if [[ -n "$MOUNT_HOME_ARGS" ]]; then
    echo "mounting $HOME as $HOME"
fi
echo "mounting $TOP as $WORKDIR"

export PLATFORM
export PROJECT
export JOBS
export RECHECK
export UNIT_TEST_ONLY
export QUICK_CHECK
export BUILD_DIR
export chain_lint

if [[ "$INSTALL_ONLY" == "t" ]]; then
    docker run --rm \
        --workdir=$WORKDIR \
        --volume=$TOP:$WORKDIR \
        ${PLATFORM} \
        ${BUILD_IMAGE} \
        sh -c "./autogen.sh &&
               ./configure --prefix=/usr --sysconfdir=/etc \
                --with-systemdsystemunitdir=/etc/systemd/system \
                --localstatedir=/var &&
               make clean &&
               make -j${JOBS}"
    RC=$?
    docker rm tmp.$$
    test $RC -ne 0 &&  die "docker run of 'make install' failed"
else
    docker run --rm \
        --workdir=$WORKDIR \
        --volume=$TOP:$WORKDIR \
        --mount type=tmpfs,destination=/test/tmpfs-1m,tmpfs-size=1048576 \
        ${PLATFORM} \
        $MOUNT_HOME_ARGS \
        -e PLATFORM \
        -e CC \
        -e CXX \
        -e LDFLAGS \
        -e CFLAGS \
        -e CPPFLAGS \
        -e GCOV \
        -e TEST_INSTALL \
        -e CPPCHECK \
        -e RECHECK \
        -e UNIT_TEST_ONLY \
        -e QUICK_CHECK \
        -e chain_lint \
        -e JOBS \
        -e USER \
	-e PROJECT \
        -e CI \
        -e PYTHON \
        -e PYTHON_VERSION \
        -e PRELOAD \
        -e BUILD_DIR \
        --cap-add SYS_PTRACE \
        --tty \
        ${INTERACTIVE:+--interactive} \
        --network=host \
        ${BUILD_IMAGE} \
        ${INTERACTIVE:-./src/test/checks_run.sh ${CMAKE_ARGS}} \
    || die "docker run failed"
fi

if test -n "$TAG"; then
    # Re-run 'make install' in fresh image, otherwise we get all
    # the context from the build above
    docker run --name=tmp.$$ \
        --workdir=${WORKDIR}/${BUILD_DIR} \
        --volume=$TOP:${WORKDIR} \
        --user="root" \
        ${PLATFORM} \
	${BUILD_IMAGE} \
	sh -c "make install && \
               userdel $USER" \
	|| (docker rm tmp.$$; die "docker run of 'make install' failed")
    docker commit tmp.$$ $TAG \
	|| die "docker commit failed"
    docker rm tmp.$$
    echo "Tagged image $TAG"
fi
