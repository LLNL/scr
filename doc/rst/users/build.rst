.. _sec-library:

Build SCR
=========

Dependencies
------------

SCR has several required dependencies.
Others are optional, and if not available,
corresponding SCR functionality is disabled.

Required:

* CMake, Version 2.8+
* Compilers (C, C++, and Fortran)
* MPI 3.0+
* pdsh (https://github.com/chaos/pdsh)
* DTCMP (https://github.com/llnl/dtcmp)
* LWGRP (https://github.com/llnl/lwgrp)
* AXL (https://github.com/ECP-VeloC/AXL)
* er (https://github.com/ECP-VeloC/er)
* KVTree (https://github.com/ECP-VeloC/KVTree)
* rankstr (https://github.com/ECP-VeloC/rankstr)
* redset (https://github.com/ECP-VeloC/redset)
* shuffile (https://github.com/ECP-VeloC/shuffile)
* spath (https://github.com/ECP-VeloC/spath)

Optional:

* libyogrt (for time remaining in a job allocation) (https://github.com/llnl/libyogrt)
* MySQL (for logging SCR activities)

To simplify the install process,
one can use the SCR :code:`bootstrap.sh` script with CMake or use Spack.

The CMake and Spack sections below assume that one is installing SCR on a system with
existing compilers, a resource manager (like SLURM or LSF), and an MPI environment.
These base software packages are typically preinstalled and configured
for users by the support staff of HPC clusters.

For those who are installing SCR outside of an HPC cluster,
are using Fedora, and have sudo access,
the following steps install and activate most of the necessary base dependencies:

.. code-block:: bash

    sudo dnf groupinstall "Development Tools"
    sudo dnf install cmake gcc-c++ mpi mpi-devel environment-modules zlib-devel pdsh
    [restart shell]
    module load mpi

.. _sec-build-cmake:

CMake
-----

SCR requires CMake version 2.8 or higher.
The SCR build uses the CMake FindMPI module to link with MPI.
This module looks for the standard :code:`mpicc` compiler wrapper,
which must be in your :code:`PATH`.

The quick version of building SCR with CMake is:

.. code-block:: bash

  git clone git@github.com:llnl/scr.git
  cd scr

  ./bootstrap.sh

  mkdir build
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=../install ..
  make install

Some useful CMake command line options are:

* :code:`-DCMAKE_INSTALL_PREFIX=[path]`: Place to install the SCR library
* :code:`-DCMAKE_BUILD_TYPE=[Debug/Release]`: Build with debugging or optimizations

* :code:`-DSCR_RESOURCE_MANAGER=[SLURM/APRUN/PMIX/LSF/NONE]`
* :code:`-DSCR_ASYNC_API=[INTEL_CPPR/NONE]`

* :code:`-DSCR_CNTL_BASE=[path]` : Path to SCR Control directory, defaults to :code:`/dev/shm`
* :code:`-DSCR_CACHE_BASE=[path]` : Path to SCR Cache directory, defaults to :code:`/dev/shm`
* :code:`-DSCR_CONFIG_FILE=[path]` : Path to SCR system configuration file, defaults to :code:`/etc/scr/scr.conf`

For setting the default logging parameters:

* :code:`-DSCR_LOG_ENABLE=[0/1]` : Whether to enable SCR logging of any type (1) or not (0), defaults to :code:`0`
* :code:`-DSCR_LOG_SYSLOG_ENABLE=[0/1]` : Whether to enable SCR logging via syslog (1) or not (0), defaults to :code:`1`
* :code:`-DSCR_LOG_SYSLOG_FACILITY=[facility]` : Facility for syslog messages (see man openlog), defaults to :code:`LOG_LOCAL7`
* :code:`-DSCR_LOG_SYSLOG_LEVEL=[level]` : Level for syslog messages (see man openlog), defaults to :code:`LOG_INFO`
* :code:`-DSCR_LOG_SYSLOG_PREFIX=[str]` : Prefix string to prepend to syslog messages, defaults to :code:`SCR`
* :code:`-DSCR_LOG_TXT_ENABLE=[0/1]` : Whether to enable SCR logging to a text file (1) or not (0), defaults to :code:`1`

If one has installed SCR dependencies in different directories,
there are CMake options to specify the path to each as needed, e.g.,:

* :code:`-DBUILD_PDSH=[OFF/ON]`: CMake can automatically download and build the PDSH dependency
* :code:`-DWITH_PDSH_PREFIX=[path to PDSH]`: Path to an existing PDSH installation (should not be used with :code:`BUILD_PDSH`)

* :code:`-DWITH_DTCMP_PREFIX=[path to DTCMP]`
* :code:`-DWITH_AXL_PREFIX=[path to AXL]`
* :code:`-DWITH_ER_PREFIX=[path to er]`
* :code:`-DWITH_KVTREE_PREFIX=[path to KVTree]`
* :code:`-DWITH_RANKSTR_PREFIX=[path to rankstr]`
* :code:`-DWITH_REDSET_PREFIX=[path to redset]`
* :code:`-DWITH_SHUFFILE_PREFIX=[path to shuffile]`
* :code:`-DWITH_SPATH_PREFIX:PATH=[path to spath]`

* :code:`-DWITH_YOGRT_PREFIX:PATH=[path to libyogrt]`
* :code:`-DWITH_MYSQL_PREFIX=[path to MySQL]`

.. _sec-build-spack:

Spack
-----

If you use the `Spack <https://github.com/spack/spack>`_ package manager,
SCR and many of its dependencies have corresponding packages.

Before installing SCR with Spack,
one should first properly configure :code:`packages.yaml`.
In particular, SCR depends on the system resource manager and MPI library,
and one should define entries for those in :code:`packages.yaml`.

By default, Spack attempts to build all dependencies for SCR,
including packages such as SLURM, MPI, and OpenSSL that are already installed on most HPC systems.
It is recommended to use the system-installed software when possible.
This ensures that the resulting SCR build actually works on the target system,
and it can significantly reduce the build time.

Spack uses its :code:`packages.yaml` file to locate external packages.
Full information about :code:`packages.yaml` can be found
in the `Spack documentation <https://spack.readthedocs.io/en/latest/configuration.html>`_.

At minimum, it is important to register the system MPI library and the system resource manager.
Other packages can be defined to accelerate the build.
The following shows example entries for :code:`packages.yaml`.
One must modify these example entries to use the proper versions,
module names, and paths for the target system:

.. code-block:: yaml

    packages:
      all:
        providers:
          mpi: [mvapich2,openmpi,spectrum-mpi]

      # example entry for MVAPICH2 MPI, accessed by a module named mvapich2
      mvapich2:
        buildable: false
        externals:
        - spec: mvapich2
          modules:
          - mvapich2

      # example entry for Open MPI
      openmpi:
        buildable: false
        externals:
        - spec: openmpi@4.1.0
          prefix: /opt/openmpi-4.1.0

      # example entry for IBM Spectrum MPI
      spectrum-mpi:
        buildable: false
        externals:
        - spec: spectrum-mpi
          prefix: /opt/ibm/spectrum_mpi

      # example entry for IBM LSF resource manager
      lsf:
        buildable: false
        externals:
        - spec: lsf@10.1
          prefix: /opt/ibm/spectrumcomputing/lsf/10.1

      # example entry for SLURM resource manager
      slurm:
        buildable: false
        externals:
        - spec: slurm@20
          prefix: /usr

      openssl:
        externals:
        - spec: openssl@1.0.2
          prefix: /usr

      libyogrt:
        externals:
        - spec: libyogrt scheduler=lsf
          prefix: /usr
        - spec: libyogrt scheduler=slurm
          prefix: /usr

The `packages` key declares the following block as a set of package descriptions.
The following descriptions tell Spack how to find items that already installed on the system.

* The `providers` key specifies that one of three different MPI versions are available, MVAPICH2, Open MPI, or IBM Spectrum MPI.

* :code:`mvapich2`: declares that MVAPICH2 is available, and the location is defined in a `mvapich2` module file.
* :code:`openmpi`: declares that Open MPI is installed in the system at the location specified by `prefix`, and the `buildable: false` line declares that Spack should always use that version of MPI rather than try to build its own. This description addresses the common situation where MPI is customized and optimized for the local system, and Spack should never try to compile a replacement.
* :code:`spectrum-mpi`: declares that Spectrum MPI is available.
* :code:`lsf`: declares that if LSF is needed (e.g. to use `scheduler=lsf`) the libraries can be found at the specified `prefix`.
* :code:`slurm`: declares that if SLURM is needed (e.g. to use `scheduler=slurm`) the libraries can be found at the specified `prefix`.
* :code:`openssl`: declares that `openssl` version 1.0.2 is installed on the system and that Spack should use that if it satisfies the dependencies required by any spack-installed packages, but if a different version is requested, Spack should install its own version.
* :code:`libyogrt`: declares that libyogrt is installed, but Spack may decide to build its own version. If `scheduler=slurm` or `scheduler=lsf` is selected, use the version installed under /usr, otherwise build from scratch using the selected scheduler.

After configuring :code:`packages.yaml`, one can install SCR.

For SLURM systems, SCR can be installed with:

.. code-block:: bash

  spack install scr@develop resource_manager=SLURM

For LSF, systems, SCR can be installed with:

.. code-block:: bash

  spack install scr@develop resource_manager=LSF

The SCR Spack package provides other variants that may be useful.
To see the full list, type:

.. code-block:: bash

  spack info scr
