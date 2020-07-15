.. _sec-library:

Build SCR
=========

Dependencies
------------

SCR has several dependencies. A C compiler, MPI, CMake, and pdsh are
required, as are several component libraries from the ECP VeloC project. The others are optional, and when they are
not available some features of SCR may not be available.

Required:

* CMake, Version 2.8+
* Compiler (C, C++, and Fortran)
* MPI
* pdsh (https://github.com/grondo/pdsh)
* AXL (https://github.com/ECP-VeloC/AXL)
* KVTree (https://github.com/ECP-VeloC/KVTree)
* er (https://github.com/ECP-VeloC/er)
* redset (https://github.com/ECP-VeloC/redset)
* rankstr (https://github.com/ECP-VeloC/rankstr)
* shuffile (https://github.com/ECP-VeloC/shuffile)
* spath (https://github.com/ECP-VeloC/spath)

Optional:

* DTCMP (for user-defined directory structure feature)(https://github.com/llnl/dtcmp)
* LWGRP (for user-defined directory structure feature) (https://github.com/LLNL/lwgrp)
* libyogrt (for determining length of time left in the current allocation) (https://github.com/llnl/libyogrt)

On Fedora, you can install the required dependencies with::

  sudo dnf groupinstall "Development Tools"
  sudo dnf install cmake gcc-c++ mpi mpi-devel environment-modules zlib-devel pdsh
  [restart shell]
  module load mpi

Spack
-----

The most automated way to build SCR is to use the Spack
package manager (https://github.com/spack/spack).
SCR and all of its dependencies exist in a Spack package. After downloading
Spack, simply type::

  spack compiler find --scope site
  spack install scr@develop resource_manager=SLURM

This will install the component libraries, DTCMP, LWGRP, libyogrt, and pdsh packages (and possibly an MPI and a
C compiler if needed).

By default, Spack will attempt to build all dependencies for SCR, including many packages such as MPI and OpenSSL that are already installed on many systems. Often it is preferrable to use the pre-installed versions. Spack uses file called 'packages.yaml' to tell it where to find external packages. Information on setting up 'packages.yaml' is available in the Spack documentation (see https://spack.readthedocs.io/en/latest/configuration.html). Spack searches for the packages.yaml file in several locations. When installing Spack in user space, customizations can be placed in $SPACK/etc/packages.yaml or $HOME/.spack/packages.yaml.

Here is an example of a simple `packages.yaml` file::

    packages:
      openmpi:
        buildable: false
      openssl:
        externals:
        - spec: openssl@1.0.2
          prefix: /usr
      libyogrt:
        externals:
        - spec: libyogrt
          prefix: /usr

The `packages` key declares the following block as a set of package descriptions. The following descriptions tell Spack how to find items that already installed on the system.

* :code:`openmpi`:, declares that OpenMPI is installed in the system, will be found automatically by the compiler, and the `buildable: false` line declares that Spack should always use that version of MPI rather than try to build its own. This description addresses the common situation where MPI is customized and optimized for the local system, and Spack should never try to compile a replacement. 
* :code:`openssl`: declares that `openssl` version 1.0.2 is installed on the system and that Spack should use that if it satisfies the dependencies required by any spack-installed packages, but if a different version is requested, Spack should install its own version.
* :code:`libyogrt`: declares that libyogrt is installed, but Spack may decide to build its own version.


CMake
-----

To get started with CMake (version 2.8 or higher), the quick version of
building SCR is::

  git clone git@github.com:llnl/scr.git
  cd scr
  mkdir build install deps
  ./bootstrap.sh

  cd build
  cmake -DCMAKE_INSTALL_PREFIX=../install ..
  make
  make install
  make test

Some useful CMake command line options are:

* :code:`-DCMAKE_INSTALL_PREFIX=[path]`: Place to install the SCR library
* :code:`-DCMAKE_BUILD_TYPE=[Debug/Release]`: Build with debugging or optimizations
* :code:`-DBUILD_PDSH=[OFF/ON]`: CMake can automatically download and build the PDSH dependency
* :code:`-DWITH_PDSH_PREFIX=[path to PDSH]`: Path to an existing PDSH installation (should not be used with :code:`BUILD_PDSH`)
* :code:`-DWITH_DTCMP_PREFIX=[path to DTCMP]`
* :code:`-DWITH_YOGRT_PREFIX=[path to YOGRT]`
* :code:`-DSCR_ASYNC_API=[CRAY_DW/INTEL_CPPR/IBM_BBAPI/NONE]`
* :code:`-DSCR_RESOURCE_MANAGER=[SLURM/APRUN/PMIX/LSF/NONE]`
* :code:`-DSCR_CNTL_BASE=[path]` : Path to SCR Control directory, defaults to :code:`/dev/shm`
* :code:`-DSCR_CACHE_BASE=[path]` : Path to SCR Cache directory, defaults to :code:`/dev/shm`
* :code:`-DSCR_CONFIG_FILE=[path]` : Path to SCR system configuration file, defaults to :code:`/etc/scr/scr.conf`
