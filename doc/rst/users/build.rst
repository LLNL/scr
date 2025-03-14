.. _sec-library:

Build SCR
=========

Dependencies
------------

SCR has several required dependencies.
Others are optional, and if not available,
corresponding SCR functionality is disabled.

Required:

* C and C++ compilers
* CMake, Version 3.14.5+
* MPI 3.0+

Optional:

* Fortran compiler (for Fortran bindings)
* pdsh (for scalable restart and scavenge) (https://github.com/chaos/pdsh)
* libyogrt (for time remaining in a job allocation) (https://github.com/llnl/libyogrt)
* MySQL (for logging SCR activities)

To simplify the install process,
one can use CMake or use Spack.

The CMake and Spack sections below assume that one is installing SCR on a system with
existing compilers, a resource manager (like SLURM or LSF), and an MPI environment.
These base software packages are typically preinstalled and configured
for users by the support staff of HPC clusters.

.. _sec-build-cmake:

CMake
-----

SCR requires CMake version 3.14.5 or higher.
The SCR build uses the CMake FindMPI module to link with MPI.
This module looks for the standard :code:`mpicc` compiler wrapper,
which must be in your :code:`PATH`.

One can download an SCR release tarball from the `GitHub release page <https://github.com/llnl/scr/releases>`_.
To build SCR from a release tarball:

.. code-block:: bash

  wget https://github.com/LLNL/scr/releases/download/v3.1.0/scr-v3.1.0.tgz
  tar -zxf scr-v3.1.0.tgz
  cd scr-v3.1.0

  mkdir build
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=../install ..
  make -j install

Some common CMake command line options:

* :code:`-DCMAKE_INSTALL_PREFIX=[path]`: Place to install the SCR library
* :code:`-DCMAKE_BUILD_TYPE=[Debug/Release]`: Build with debugging or optimizations, defaults to :code:`Release`
* :code:`-DBUILD_SHARED_LIBS=[ON/OFF]`: Whether to build shared libraries, defaults to :code:`ON`

* :code:`-DSCR_RESOURCE_MANAGER=[SLURM/FLUX/APRUN/LSF/NONE]` : Resource manager for job allocations, defaults to :code:`SLURM`

* :code:`-DSCR_CNTL_BASE=[path]` : Path to SCR Control directory, defaults to :code:`/dev/shm`
* :code:`-DSCR_CACHE_BASE=[path]` : Path to SCR Cache directory, defaults to :code:`/dev/shm`
* :code:`-DSCR_CONFIG_FILE=[path]` : Path to SCR system configuration file, defaults to :code:`<install>/etc/scr.conf`

* :code:`-DSCR_FILE_LOCK=[FLOCK/FCNTL/NONE]` : Specify type of file locking to use, defaults to :code:`FLOCK`

For setting the default logging parameters:

* :code:`-DSCR_LOG_ENABLE=[0/1]` : Whether to enable SCR logging of any type (1) or not (0), defaults to :code:`0`
* :code:`-DSCR_LOG_SYSLOG_ENABLE=[0/1]` : Whether to enable SCR logging via syslog (1) or not (0), defaults to :code:`1`
* :code:`-DSCR_LOG_SYSLOG_FACILITY=[facility]` : Facility for syslog messages (see man openlog), defaults to :code:`LOG_LOCAL7`
* :code:`-DSCR_LOG_SYSLOG_LEVEL=[level]` : Level for syslog messages (see man openlog), defaults to :code:`LOG_INFO`
* :code:`-DSCR_LOG_SYSLOG_PREFIX=[str]` : Prefix string to prepend to syslog messages, defaults to :code:`SCR`
* :code:`-DSCR_LOG_TXT_ENABLE=[0/1]` : Whether to enable SCR logging to a text file (1) or not (0), defaults to :code:`1`

One can disable portions of the SCR build if they are not needed:

* :code:`-DENABLE_FORTRAN=[ON/OFF]` : Whether to build library for Fortran bindings, defaults to :code:`ON`
* :code:`-DENABLE_FORTRAN_TRAILING_UNDERSCORES=[AUTO/ON/OFF]` : Whether to append underscores to symbol names in the Fortran bindings, defaults to :code:`AUTO`
* :code:`-DENABLE_EXAMPLES=[ON/OFF]` : Whether to build programs in :code:`examples` directory, defaults to :code:`ON`
* :code:`-DENABLE_TESTS=[ON/OFF]` : Whether to support :code:`make check` tests, defaults to :code:`ON`

* :code:`-DENABLE_PTHREADS=[ON/OFF]` : Whether to enable pthreads support for file transfers, defaults to :code:`ON`
* :code:`-DENABLE_IBM_BBAPI=[ON/OFF]` : Whether to enable IBM Burst Buffer support for file transfers, defaults to :code:`OFF`
* :code:`-DENABLE_CRAY_DW=[ON/OFF]` : Whether to enable Cray DataWarp support for file transfers, defaults to :code:`OFF`

* :code:`-DENABLE_PDSH=[ON/OFF]` : Whether to use pdsh to check node health and scavenge files, defalts to :code:`ON`
* :code:`-DBUILD_PDSH=[ON/OFF]`: CMake can automatically download and build the PDSH dependency, defaults to :code:`OFF`
* :code:`-DWITH_PDSH_PREFIX=[path to PDSH]`: Path to an existing PDSH installation (should not be used with :code:`BUILD_PDSH`)

* :code:`-DENABLE_YOGRT=[ON/OFF]` : Whether to use libyogrt for determining allocation end time, defaults to :code:`ON`
* :code:`-DWITH_YOGRT_PREFIX:PATH=[path to libyogrt]`

* :code:`-DENABLE_MYSQL=[ON/OFF]` : Whether to use MySQL for logging, defaults to :code:`OFF`
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

  spack install scr@3.0 resource_manager=SLURM

For LSF, systems, SCR can be installed with:

.. code-block:: bash

  spack install scr@3.0 resource_manager=LSF

The SCR Spack package provides other variants that may be useful.
To see the full list, type:

.. code-block:: bash

  spack info scr
