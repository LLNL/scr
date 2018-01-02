.. _sec-library:

Build SCR
=========

Dependencies
------------

SCR has several dependencies. A C compiler, MPI, CMake, and pdsh are 
required dependencies. The others are optional, and when they are 
not available some features of SCR may not be available.

* CMake, Version 2.8+
* Compiler (C, C++, and Fortran)
* MPI
* pdsh (https://github.com/grondo/pdsh)
* DTCMP (optional, used for user-defined directory structure feature)(https://github.com/llnl/dtcmp)
* LWGRP (optional, used for user-defined directory structure feature) (https://github.com/LLNL/lwgrp)
* libyogrt (optional, used for determining length of time left in the current allocation) (https://github.com/llnl/libyogrt)
* MySQL (optional, used for logging SCR activities)

Spack
-----

The most automated way to build SCR is to use the Spack
package manager (https://github.com/spack/spack).
SCR and all of its dependencies exist in a Spack package. After downloading
Spack, simply type::

  spack install scr

This will install the DTCMP, LWGRP, and pdsh packages (and possibly an MPI and a C compiler if needed).

CMake
-----

To get started with CMake (version 2.8 or higher), the quick version of 
building SCR is::

  git clone git@github.com:llnl/scr.git
  mkdir build
  mkdir install
  
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=../install ../scr
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
* :code:`-DSCR_CNTL_BASE=[path]` : Path to SCR Control directory, defaults to :code:`/tmp`
* :code:`-DSCR_CACHE_BASE=[path]` : Path to SCR Cache directory, defaults to :code:`/tmp`
* :code:`-DSCR_CONFIG_FILE=[path]` : Path to SCR system configuration file, defaults to :code:`/etc/scr/scr.conf`

To change the SCR control directory, one must either set :code:`-DSCR_CNTL_BASE` at build time
or one must specify :code:`SCR_CNTL_BASE` in the SCR system configuration file.
These paths are hardcoded into the SCR library and scripts during the build process.
It is not possible to specify the control directory through environment variables or the user configuration file.

Unlike the control directory, the SCR cache directory can be specified at run time
through either environment variables or the user configuration file.
