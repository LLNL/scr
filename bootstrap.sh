#!/bin/bash
#
# This is an easy-bake script to download and build all SCR's external
# dependencies.  The dependencies will be build in scr/deps/ and
# installed to scr/install/
#

ROOT="$(pwd)"

mkdir -p deps
mkdir -p install
INSTALL_DIR=$ROOT/install

cd deps

repos=(https://github.com/ECP-Veloc/KVTree.git
    https://github.com/ECP-Veloc/AXL.git
    https://github.com/ECP-Veloc/spath.git
    https://github.com/ECP-Veloc/rankstr.git
    https://github.com/ECP-Veloc/redset.git
    https://github.com/ECP-Veloc/shuffile.git
    https://github.com/ECP-Veloc/er.git
    https://github.com/ECP-Veloc/filo.git
)

for i in "${repos[@]}" ; do
	# Get just the name of the project (like "mercury")
	name=$(basename $i | sed 's/\.git//g')
	if [ -d $name ] ; then
		echo "$name already exists, skipping it"
	else
		if [ "$name" == "mercury" ] ; then
			git clone --recurse-submodules $i
		else
			git clone $i
		fi
	fi
done

cd KVTree
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON ..
make -j `nproc`
make install
cd ../..

cd AXL
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DAXL_ASYNC_DAEMON=OFF -DMPI=ON ..
make -j `nproc`
make install
cd ../..

# spath
cd spath
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON ..
make -j `nproc`
make install
#make test
cd ../..

# rankstr
cd rankstr
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DMPI=ON ..
make -j `nproc`
make install
#make test
cd ../..

cd redset
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DWITH_KVTREE_PREFIX=$INSTALL_DIR -DMPI=ON ..
make -j `nproc`
make install
cd ../..

cd shuffile
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DWITH_KVTREE_PREFIX=$INSTALL_DIR -DMPI=ON ..
make -j `nproc`
make install
cd ../..

cd er
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DWITH_KVTREE_PREFIX=$INSTALL_DIR -DWITH_REDSET_PREFIX=$INSTALL_DIR -DWITH_SHUFFILE_PREFIX=$INSTALL_DIR -DMPI=ON ..
make -j `nproc`
make install
cd ../..

cd filo
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR -DWITH_KVTREE_PREFIX=$INSTALL_DIR -DWITH_AXL_PREFIX=$INSTALL_DIR -DMPI=ON ..
make -j `nproc`
make install
cd ../..

cd "$ROOT"
mkdir -p build
echo "*************************************************************************"
echo "Dependencies are all built.  You can now build SCR with:"
echo ""
echo "  mkdir -p build && cd build"
echo "  cmake -DCMAKE_INSTALL_PREFIX=$INSTALL_DIR .."
echo "  make && make install"
echo "*************************************************************************"
