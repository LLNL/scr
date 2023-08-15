#!/bin/bash
#
# This is an easy-bake script to download and build all SCR's external
# dependencies.  The dependencies will be build in scr/deps/ and
# installed to scr/install/
#
# Optional flags:
#   --ssh     clone dependency repos with git ssh instead of https
#   --debug   compiles dependencies with full debug "-g -O0"
#   --opt     compiles dependencies with optimization (non-debug)
#   --dev     builds each dependency from its latest commit
#   --tag     builds each dependency from a hardcoded tag
#   --clean   deletes deps and install directories before build
#   --verbose build with VERBOSE=1 flag set to capture more build output
#   --static  build static libraries instead of shared libraries
#   --noshelldbg run script without "-x"
#

# optional builds
clone_ssh=0     # whether to clone with https (0) or ssh (1)
build_debug=0   # whether to build optimized (0) or debug "-g -O0" (1)
build_dev=1     # whether to checkout fixed version tags (0) or use latest (1)
build_clean=0   # whether to keep deps directory (0) or delete and recreate (1)
make_verbose=0  # whether to run make with "VERBOSE=1" or not
shared_flags="-DBUILD_SHARED_LIBS=ON"
build_with_shell_dbg=1

while [ $# -ge 1 ]; do
  case "$1" in
    "--ssh" )
      clone_ssh=1 ;;
    "--debug" )
      build_debug=1 ;;
    "--opt" )
      build_debug=0 ;;
    "--dev" )
      build_dev=1 ;;
    "--tag" )
      build_dev=0 ;;
    "--clean" )
      build_clean=1 ;;
    "--verbose" )
      make_verbose=1 ;;
    "--static" )
      shared_flags="-DBUILD_SHARED_LIBS=OFF" ;;
    "--noshelldbg" )
      build_with_shell_dbg=0 ;;
    *)
      echo "USAGE ERROR: unknown option $1"
      exit 1 ;;
  esac
  shift
done

if [ ${build_with_shell_dbg} = 1 ]; then
    set -x 
fi

run_cmd() {
    echo $1
    if ! eval $1 ; then
        echo "FAIL: See ${log_file} for details"
        exit 1
    fi
}

ROOT="$(pwd)"
INSTALL_DIR=$ROOT/install

if [ $build_clean -eq 1 ] ; then
  run_cmd "rm -rf deps"
  run_cmd "rm -rf install"
fi

run_cmd "mkdir -p deps"
run_cmd "mkdir -p install"

run_cmd "cd deps"

lwgrp=lwgrp-1.0.5
dtcmp=dtcmp-1.1.4
pdsh=pdsh-2.34

if [ ! -f ${lwgrp}.tar.gz ] ; then
  run_cmd "wget https://github.com/LLNL/lwgrp/releases/download/v1.0.5/${lwgrp}.tar.gz"
fi
if [ ! -f ${dtcmp}.tar.gz ] ; then
  run_cmd "wget https://github.com/LLNL/dtcmp/releases/download/v1.1.4/${dtcmp}.tar.gz"
fi
if [ ! -f ${pdsh}.tar.gz ] ; then
  run_cmd "wget https://github.com/chaos/pdsh/releases/download/${pdsh}/${pdsh}.tar.gz"
fi

if [ $clone_ssh -eq 0 ] ; then
  # clone with https
  url_prefix="https://github.com/"
else
  # clone with ssh (requires user to have their ssh keys registered on github)
  url_prefix="git@github.com:"
fi
repos=(${url_prefix}ECP-VeloC/KVTree.git
  ${url_prefix}ECP-VeloC/AXL.git
  ${url_prefix}ECP-VeloC/spath.git
  ${url_prefix}ECP-VeloC/rankstr.git
  ${url_prefix}ECP-VeloC/redset.git
  ${url_prefix}ECP-VeloC/shuffile.git
  ${url_prefix}ECP-VeloC/er.git
)

for i in "${repos[@]}" ; do
  # Get just the name of the project (like "KVTree")
  name=$(basename $i | sed 's/\.git//g')
  if [ -d $name ] ; then
    echo "$name already exists, skipping it"
  else
    run_cmd "git clone $i"
  fi
done

# whether to build optimized or "-g -O0" debug
buildtype="Release"
if [ $build_debug -eq 1 ] ; then
  buildtype="Debug"
fi

make_cmd=""
if [ ${make_verbose} = 1 ]; then
    make_cmd="make VERBOSE=1 install"
else
    make_cmd="make -j $( nproc ) install"
fi

run_cmd "rm -rf ${lwgrp}"
run_cmd "tar -zxf ${lwgrp}.tar.gz"
run_cmd "pushd ${lwgrp}"
  run_cmd "./configure --prefix=${INSTALL_DIR} && ${make_cmd}"
run_cmd "popd"

run_cmd "rm -rf ${dtcmp}"
run_cmd "tar -zxf ${dtcmp}.tar.gz"
run_cmd "pushd ${dtcmp}"
  run_cmd "./configure --prefix=${INSTALL_DIR} --with-lwgrp=${INSTALL_DIR} && ${make_cmd}"
run_cmd "popd"

run_cmd "rm -rf ${pdsh}"
run_cmd "tar -zxf ${pdsh}.tar.gz"
run_cmd "pushd ${pdsh}"
  run_cmd "./configure --prefix=$INSTALL_DIR && ${make_cmd}"
run_cmd "popd"

run_cmd "pushd KVTree"
  if [ $build_dev -eq 0 ] ; then
    run_cmd "git checkout v1.3.0"
  fi
  run_cmd "rm -rf build"
  run_cmd "mkdir -p build"
  run_cmd "pushd build"
    run_cmd "cmake ${shared_flags} -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON .. && ${make_cmd}"
  run_cmd "popd"
run_cmd "popd"

pushd AXL
  if [ $build_dev -eq 0 ] ; then
    run_cmd "git checkout v0.6.0"
  fi
  run_cmd "rm -rf build"
  run_cmd "mkdir -p build"
  run_cmd "pushd build"
    run_cmd "CMAKE_PREFIX_PATH='/usr/global/tools/nnfdm_x86_64/current' cmake ${shared_flags} -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON .. && ${make_cmd}"
  run_cmd "popd"
run_cmd "popd"

run_cmd "pushd spath"
  if [ $build_dev -eq 0 ] ; then
    run_cmd "git checkout v0.2.0"
  fi
  run_cmd "rm -rf build"
  run_cmd "mkdir -p build"
  run_cmd "pushd build"
    run_cmd "cmake ${shared_flags} -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON .. && ${make_cmd}"
  run_cmd "popd"
run_cmd "popd"

run_cmd "pushd rankstr"
  if [ $build_dev -eq 0 ] ; then
    run_cmd "git checkout v0.2.0"
  fi
  run_cmd "rm -rf build"
  run_cmd "mkdir -p build"
  run_cmd "pushd build"
    run_cmd "cmake ${shared_flags} -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .. && ${make_cmd}"
  run_cmd "popd"
run_cmd "popd"

run_cmd "pushd redset"
  if [ $build_dev -eq 0 ] ; then
    run_cmd "git checkout v0.2.0"
  fi
  run_cmd "rm -rf build"
  run_cmd "mkdir -p build"
  run_cmd "pushd build"
    run_cmd "cmake ${shared_flags} -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .. && ${make_cmd}"
  run_cmd "popd"
run_cmd "popd"

run_cmd "pushd shuffile"
  if [ $build_dev -eq 0 ] ; then
    run_cmd "git checkout v0.2.0"
  fi
  run_cmd "rm -rf build"
  run_cmd "mkdir -p build"
  run_cmd "pushd build"
    run_cmd "cmake ${shared_flags} -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .. && ${make_cmd}"
  run_cmd "popd"
run_cmd "popd"

run_cmd "pushd er"
  if [ $build_dev -eq 0 ] ; then
    run_cmd "git checkout v0.2.0"
  fi
  run_cmd "rm -rf build"
  run_cmd "mkdir -p build"
  run_cmd "pushd build"
    run_cmd "cmake ${shared_flags} -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .. && ${make_cmd}"
  run_cmd "popd"
run_cmd "popd"

if [ ${build_with_shell_dbg} = 1 ]; then
  set +x
fi
run_cmd "cd "$ROOT""
run_cmd "mkdir -p build"
echo "*************************************************************************"
echo "Dependencies are all built.  You can now build SCR with:"
echo ""
echo "  mkdir -p build && cd build"
echo "  cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .."
echo "  ${make_cmd}"
echo "*************************************************************************"
