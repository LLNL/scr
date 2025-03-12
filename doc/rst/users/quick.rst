.. _sec-quick:

Quick Start
===========

In this quick start guide, we assume that you already have a basic
understanding of SCR and how it works on HPC systems.
We also assume you have access to a SLURM cluster with a few compute nodes
and a working MPI environment.
We walk through a bare bones example to quickly get you started with SCR.
For more in-depth information,
see subsequent sections in this user's guide.

Building SCR
------------

SCR has a number of dependencies.
To simplify the install process,
one can use CMake or Spack.
For full details on building SCR,
please see Section :ref:`sec-library`.

CMake
^^^^^

SCR requires CMake version 3.14.5 or higher.
The SCR build uses the CMake FindMPI module to link with MPI.
This module looks for the standard :code:`mpicc` compiler wrapper,
which must be in your :code:`PATH`.

To download and build SCR with CMake:

.. code-block:: bash

  wget https://github.com/LLNL/scr/releases/download/v3.1.0/scr-v3.1.0.tgz
  tar -zxf scr-v3.1.0.tgz
  cd scr-v3.1.0

  mkdir build install
  cd build
  cmake -DCMAKE_INSTALL_PREFIX=../install ..
  make -j install

There are a number of CMake options to configure the build.
For more details, see Section :ref:`sec-build-cmake`.

Spack
^^^^^

If you use the `Spack <https://github.com/spack/spack>`_ package manager,
SCR and many of its dependencies have corresponding packages.

Before installing SCR with Spack,
one should first configure :code:`packages.yaml`.
In particular, SCR depends on the system resource manager and MPI library,
and one should define entries for those in :code:`packages.yaml`.
Examples for configuring common resource managers and MPI libraries
are listed in Section :ref:`sec-build-spack`.

SCR can then be installed for SLURM systems with:

.. code-block:: bash

  spack install scr@3.1.0

This downloads, builds, and installs SCR and its dependencies.

Building the SCR :code:`test_api` Example
-------------------------------------------

In this quick start guide, we use the :code:`test_api.c` program.

If you install SCR with CMake,
:code:`test_api.c` is compiled as part of the make install step.
You can find it in the :code:`examples` subdirectory
within the CMake :code:`build` directory:

.. code-block:: bash

  cd examples

If you still have this direcotry,
then skip ahead to the next section to run :code:`test_api.c`.

Alternatively, you will find source files for example programs
in the :code:`<install>/share/scr/examples` directory,
where :code:`<install>` is the path in which SCR was installed.

If you install SCR with Spack,
you can identify the SCR install directory with the following command:

.. code-block:: bash

  spack location -i scr

Then build :code:`test_api.c` by executing:

.. code-block:: bash

  cd <install>/share/scr/examples
  make test_api

Upon a successful build, you will have a :code:`test_api` executable.

Running the SCR :code:`test_api` Example
------------------------------------------

A quick test of your SCR installation can be done by
running :code:`test_api` in an interactive job allocation.
The following assumes you are running on a SLURM-based system.
If you are not using SLURM, then modify the node allocation and
run commands as appropriate for your resource manager.

First, obtain compute nodes for testing.
Here we allocate 4 nodes:

.. code-block:: bash

  salloc -N 4

Once you have the compute nodes you can run :code:`test_api`.
Here we execute a 4-process run on 4 nodes:

.. code-block:: bash

  srun -n 4 -N 4 ./test_api

This example program writes 6 checkpoints using SCR.
Assuming all goes well, you should see output similar to the following

.. code-block:: none

  >>: srun -n 4 -N 4 ./test_api
  Init: Min 0.033856 s    Max 0.033857 s  Avg 0.033856 s
  No checkpoint to restart from
  At least one rank (perhaps all) did not find its checkpoint
  Completed checkpoint 1.
  Completed checkpoint 2.
  Completed checkpoint 3.
  Completed checkpoint 4.
  Completed checkpoint 5.
  Completed checkpoint 6.
  FileIO: Min   52.38 MB/s        Max   52.39 MB/s        Avg   52.39 MB/s       Agg  209.55 MB/s

If you do not see output similar to this,
there may be a problem with your environment or your build of SCR.
Please see the detailed sections of this user guide for more help
or email us (see :ref:`sec-contact`.)

One can use :code:`test_api` to conduct more interesting tests.
In the SCR source directory,
the :code:`testing` directory includes scripts to demonstrate different aspects of SCR.
Depending on your shell preference,
:code:`TESTING.csh` or :code:`TESTING.sh` are good for getting started.
Each script contains a sequence of additional configurations and commands for running :code:`test_api`.
One can find those :code:`TESTING` scripts in a clone of the repo, e.g.:

.. code-block:: bash

  git clone git@github.com:llnl/scr.git
  cd scr/testing

Adding SCR to Your Application
---------------------------------

Here we provide an example of integrating the SCR API
into an application to write checkpoints.

.. code-block:: c

  int main(int argc, char* argv[]) {
    MPI_Init(argc, argv);

    /* Call SCR_Init after MPI_Init */
    SCR_Init();

    for (int t = 0; t < TIMESTEPS; t++) {
      /* ... Do work ... */

      /* Ask SCR if a checkpoint should be saved (optional) */
      int need_ckpt;
      SCR_Need_checkpoint(&need_ckpt);
      if (need_ckpt)
        checkpoint(t);
    }

    /* Call SCR_Finalize before MPI_Finalize */
    SCR_Finalize();

    MPI_Finalize();

    return 0;
  }

  void checkpoint(int timestep) {
    /* Define a name for our checkpoint */
    char name[256];
    sprintf(name, "timestep.%d", timestep);

    /* Tell SCR that we are starting a checkpoint phase */
    SCR_Start_output(name, SCR_FLAG_CHECKPOINT);

    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* Define our checkpoint file name */
    char file[256];
    sprintf(file, "%s/rank_%d.ckpt", name, rank);

    /* Register our checkpoint file with SCR,
     * and obtain path we should use to open it */
    char scr_file[SCR_MAX_FILENAME];
    SCR_Route_file(file, scr_file);

    /* Each process will inform SCR whether it wrote
     * its checkpoint successfully */
    int valid = 1;

    /* Use path from SCR to open checkpoint file for writing */
    FILE* fs = fopen(scr_file, "w");
    if (fs != NULL) {
      int rc = fwrite(state, ..., fs);
      if (rc == 0)
        /* Failed to write, mark checkpoint as invalid */
        valid = 0;

      fclose(fs);
    } else {
      /* Failed to open file, mark checkpoint as invalid */
      valid = 0;
    }

    /* Tell SCR that we have finished our checkpoint phase */
    SCR_Complete_output(valid);

    return;
  }

Further sections in the user guide give more
details and demonstrate how to perform restart with SCR.
For a description of the API, see :ref:`sec-lib-api`,
and for more detailed instructions on integrating the API, see :ref:`sec-integration`.

It may also be instructive to examine the source of the
:code:`test_api.c` program and other programs in the examples directory.

Final Thoughts
--------------

This was a quick introduction to building and running with SCR.
For more information, please look at the more
detailed sections in the rest of this user guide.
