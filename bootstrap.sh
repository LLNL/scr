#!/bin/bash
#
# This is an easy-bake script to download and build all SCR's external
# dependencies.  The dependencies will be build in scr/deps/ and
# installed to scr/install/
#
# Optional flags:
#   --debug  compiles dependencies with full debug "-g -O0"
#   --dev    builds most recent version of each dependency
#

set -x 

# optional builds
build_debug=0 # whether to build optimized (0) or debug "-g -O0" (1)
build_dev=0   # whether to checkout fixed version tags (0) or use latest (1)

while [ $# -ge 1 ]; do
    case "$1" in
      "--debug" )
        build_debug=1 ;;
      "--dev" )
        build_dev=1 ;;
      *)
        echo "USAGE ERROR: unknown option $1"
        exit 1 ;;
    esac
    shift
done

ROOT="$(pwd)"

mkdir -p deps
mkdir -p install
INSTALL_DIR=$ROOT/install

cd deps

lwgrp=lwgrp-1.0.3
dtcmp=dtcmp-1.1.2
pdsh=pdsh-2.34

if [ ! -f ${lwgrp}.tar.gz ] ; then
  wget https://github.com/LLNL/lwgrp/releases/download/v1.0.3/${lwgrp}.tar.gz
fi
if [ ! -f ${dtcmp}.tar.gz ] ; then
  wget https://github.com/LLNL/dtcmp/releases/download/v1.1.2/${dtcmp}.tar.gz
fi
if [ ! -f ${pdsh}.tar.gz ] ; then
  wget https://github.com/chaos/pdsh/releases/download/${pdsh}/${pdsh}.tar.gz
fi

repos=(https://github.com/ECP-VeloC/KVTree.git
    https://github.com/ECP-VeloC/AXL.git
    https://github.com/ECP-VeloC/spath.git
    https://github.com/ECP-VeloC/rankstr.git
    https://github.com/ECP-VeloC/redset.git
    https://github.com/ECP-VeloC/shuffile.git
    https://github.com/ECP-VeloC/er.git
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

cd KVTree
  if [ $build_dev -eq 0 ] ; then
    git checkout v1.1.1
  fi
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON ..
  make -j `nproc`
  make install
cd ../..

cd AXL
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.4.0
  fi
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR ..
  make -j `nproc`
  make install
cd ../..

cd spath
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.0.2
  fi
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON ..
  make -j `nproc`
  make install
  #make test
cd ../..

cd rankstr
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.0.3
  fi
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON ..
  make -j `nproc`
  make install
  #make test
cd ../..

cd redset
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.0.5
  fi
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DWITH_KVTREE_PREFIX=$INSTALL_DIR -DMPI=ON ..
  make -j `nproc`
  make install
cd ../..

cd shuffile
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.0.4
  fi
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DWITH_KVTREE_PREFIX=$INSTALL_DIR -DMPI=ON ..
  make -j `nproc`
  make install
cd ../..

cd er
  if [ $build_dev -eq 0 ] ; then
    git checkout v0.0.4
  fi
  mkdir -p build && cd build
  cmake -DCMAKE_BUILD_TYPE=$buildtype -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DWITH_KVTREE_PREFIX=$INSTALL_DIR -DWITH_REDSET_PREFIX=$INSTALL_DIR -DWITH_SHUFFILE_PREFIX=$INSTALL_DIR -DMPI=ON ..
  make -j `nproc`
  make install
cd ../..

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
