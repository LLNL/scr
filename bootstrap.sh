#!/bin/bash
#
# This is an easy-bake script to download and build all SCR's external
# dependencies.  The dependencies will be build in scr/deps/ and
# installed to scr/install/
#
# Optional flags:
#   --ssh    clone dependency repos with git ssh instead of https
#   --debug  compiles dependencies with full debug "-g -O0"
#   --opt    compiles dependencies with optimization (non-debug)
#   --dev    builds each dependency from its latest commit
#   --tag    builds each dependency from a hardcoded tag
#   --clean  deletes deps and install directories before build
#

set -x 

# optional builds
clone_ssh=0   # whether to clone with https (0) or ssh (1)
build_debug=0 # whether to build optimized (0) or debug "-g -O0" (1)
build_dev=1   # whether to checkout fixed version tags (0) or use latest (1)
build_clean=0 # whether to keep deps directory (0) or delete and recreate (1)

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
    *)
      echo "USAGE ERROR: unknown option $1"
      exit 1 ;;
  esac
  shift
done

ROOT="$(pwd)"
INSTALL_DIR=$ROOT/install

if [ $build_clean -eq 1 ] ; then
  rm -rf deps
  rm -rf install
fi

mkdir -p deps
mkdir -p install

cd deps

lwgrp=lwgrp-1.0.5
dtcmp=dtcmp-1.1.4
pdsh=pdsh-2.34

if [ ! -f ${lwgrp}.tar.gz ] ; then
  wget https://github.com/LLNL/lwgrp/releases/download/v1.0.5/${lwgrp}.tar.gz
fi
if [ ! -f ${dtcmp}.tar.gz ] ; then
  wget https://github.com/LLNL/dtcmp/releases/download/v1.1.4/${dtcmp}.tar.gz
fi
if [ ! -f ${pdsh}.tar.gz ] ; then
  wget https://github.com/chaos/pdsh/releases/download/${pdsh}/${pdsh}.tar.gz
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
    git clone $i
  fi
done

# whether to build optimized or "-g -O0" debug
buildtype="Release"
if [ $build_debug -eq 1 ] ; then
  buildtype="Debug"
fi

rm -rf ${lwgrp}
tar -zxf ${lwgrp}.tar.gz
pushd ${lwgrp}
  ./configure \
    --prefix=${INSTALL_DIR} && \
  make && \
  make install
  if [ $? -ne 0 ]; then
    echo "failed to configure, build, or install liblwgrp"
    exit 1
  fi
popd

rm -rf ${dtcmp}
tar -zxf ${dtcmp}.tar.gz
pushd ${dtcmp}
  ./configure \
    --prefix=${INSTALL_DIR} \
    --with-lwgrp=${INSTALL_DIR} && \
  make && \
  make install
  if [ $? -ne 0 ]; then
    echo "failed to configure, build, or install libdtcmp"
    exit 1
  fi
popd

rm -rf ${pdsh}
tar -zxf ${pdsh}.tar.gz
pushd ${pdsh}
  ./configure --prefix=$INSTALL_DIR && \
  make && \
  make install
  if [ $? -ne 0 ]; then
    echo "failed to configure, build, or install pdsh"
    exit 1
  fi
popd

pushd KVTree
  if [ $build_dev -eq 0 ] ; then
    git checkout v1.3.0
  fi
  rm -rf build
  mkdir -p build
  pushd build
    cmake \
      -DCMAKE_BUILD_TYPE=$buildtype \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
      -DMPI=ON \
      .. && \
    make -j `nproc` && \
    make install
    if [ $? -ne 0 ]; then
      echo "failed to configure, build, or install kvtree"
      exit 1
    fi
  popd
popd

pushd AXL
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.6.0
  fi
  rm -rf build
  mkdir -p build
  pushd build
    cmake \
      -DCMAKE_BUILD_TYPE=$buildtype \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
      -DMPI=ON \
      .. && \
    make -j `nproc` && \
    make install
    if [ $? -ne 0 ]; then
      echo "failed to configure, build, or install axl"
      exit 1
    fi
  popd
popd

pushd spath
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.2.0
  fi
  rm -rf build
  mkdir -p build
  pushd build
    cmake \
      -DCMAKE_BUILD_TYPE=$buildtype \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
      -DMPI=ON \
      .. && \
    make -j `nproc` && \
    make install
    if [ $? -ne 0 ]; then
      echo "failed to configure, build, or install spath"
      exit 1
    fi
  popd
popd

pushd rankstr
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.2.0
  fi
  rm -rf build
  mkdir -p build
  pushd build
    cmake \
      -DCMAKE_BUILD_TYPE=$buildtype \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
      .. && \
    make -j `nproc` && \
    make install
    if [ $? -ne 0 ]; then
      echo "failed to configure, build, or install rankstr"
      exit 1
    fi
  popd
popd

pushd redset
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.2.0
  fi
  rm -rf build
  mkdir -p build
  pushd build
    cmake \
      -DCMAKE_BUILD_TYPE=$buildtype \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
      -DWITH_KVTREE_PREFIX=$INSTALL_DIR \
      .. && \
    make -j `nproc` && \
    make install
    if [ $? -ne 0 ]; then
      echo "failed to configure, build, or install redset"
      exit 1
    fi
  popd
popd

pushd shuffile
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.2.0
  fi
  rm -rf build
  mkdir -p build
  pushd build
    cmake \
      -DCMAKE_BUILD_TYPE=$buildtype \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
      -DWITH_KVTREE_PREFIX=$INSTALL_DIR \
      .. && \
    make -j `nproc` && \
    make install
    if [ $? -ne 0 ]; then
      echo "failed to configure, build, or install shuffile"
      exit 1
    fi
  popd
popd

pushd er
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.2.0
  fi
  rm -rf build
  mkdir -p build
  pushd build
    cmake \
      -DCMAKE_BUILD_TYPE=$buildtype \
      -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR \
      -DWITH_KVTREE_PREFIX=$INSTALL_DIR \
      -DWITH_REDSET_PREFIX=$INSTALL_DIR \
      -DWITH_SHUFFILE_PREFIX=$INSTALL_DIR \
      .. && \
    make -j `nproc` && \
    make install
    if [ $? -ne 0 ]; then
      echo "failed to configure, build, or install er"
      exit 1
    fi
  popd
popd

set +x
cd "$ROOT"
mkdir -p build
echo "*************************************************************************"
echo "Dependencies are all built.  You can now build SCR with:"
echo ""
echo "  mkdir -p build && cd build"
echo "  cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .."
echo "  make && make install"
echo "*************************************************************************"
