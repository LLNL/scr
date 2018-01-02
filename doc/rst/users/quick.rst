.. _sec-quick:

Quick Start
===========

In this quick start guide, we assume that you already have a basic
understanding of SCR and how it works on HPC systems. We will walk through a 
bare bones example to get you started quickly. For more in-depth
information, please see subsequent sections in this user's guide.

Obtaining the SCR Source
------------------------

The latest version of the SCR source code is kept at github:
https://github.com/LLNL/scr.
It can be cloned or you may download a release tarball.

Building SCR
------------

SCR has several dependencies. A C compiler, MPI, CMake, and pdsh are
required dependencies. The others are optional, and when they are
not available some features of SCR may not be available.
SCR uses the standard mpicc compiler wrapper in its build, so you will
need to have it in your :code:`PATH`. We assume the minimum set of 
dependencies in this quick start guide, which can be automatically
obtained with either Spack or CMake. For more help on installing 
dependencies of SCR or building SCR, please see Section :ref:`sec-library`.

Spack
^^^^^

The most automated way to build SCR is to use the Spack
package manager (https://github.com/spack/spack).
SCR and all of its dependencies exist in a Spack package. After downloading
Spack, simply type::

  spack install scr

This will download and install SCR and its dependencies automatically.

CMake
^^^^^

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

Since pdsh is required,
the :code:`WITH_PDSH_PREFIX` should be passed to CMake
if it is installed in a non-standard location. 
On most systems, MPI should automatically be detected.


Building the SCR :code:`test_api` Example
-------------------------------------------

After installing SCR, go to the installation directory, :code:`<install dir>` above. In the :code:`<install dir>/share/scr/examples` directory
you will find the example programs supplied with SCR. For this quick start
guide, we will use the :code:`test_api` program. Build it by executing::

  make test_api

Upon successful build, you will have an executable in your directory called
:code:`test_api`. You can use this test program to get a feel for how
SCR works and to ensure that your build of SCR is working.


Running the SCR :code:`test_api` Example
------------------------------------------

A quick test of your SCR installation can be done by setting a few 
environment variables in an interactive job allocation.
The following assumes you are running on a SLURM-based system.
If you are not using SLURM, then you will need  to modify
the allocation and run commands according to the resource manager 
you are using. 

First, obtain a few compute nodes for testing. 
Here we will allocate 4 compute nodes on a 
system with a queue for debugging called :code:`pdebug`::

  salloc -N 4 -p pdebug
 
Once you have the four compute nodes, you can experiment with SCR 
using the :code:`test_api` program. First set a few environment variables.
We're using csh in this example; you'll need to update the commands if
you are using a different shell.::

  # make sure the SCR library is in your library path
  setenv LD_LIBRARY_PATH ${SCR_INSTALL}/lib   
  
  # tell SCR to not flush to the parallel file system periodically
  setenv SCR_FLUSH 0

Now, we can run a simple test to see if your SCR installation is working.
Here we'll run a 4-process run on 4 nodes::

  srun -n4 -N4 ./test_api

Assuming all goes well, you should see output similar to the following

.. code-block:: none

  >>: srun -N 4 -n 4 ./test_api
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

If you did not see output similar to this, there is likely a problem
with your environment set up or build of SCR. Please see the 
detailed sections of this user guide for more help or email us (See
the Support and Contacts section of this user guide.)

If you want to get into more depth, in the SCR source directory, 
you will find a directory called :code:`testing`. In this directory,
there are various scripts we use for testing our code. Perhaps the most
useful for getting started are the :code:`TESTING.csh` or :code:`TESTING.sh`
files, depending on your shell preference. 

Getting SCR into Your Application
---------------------------------

Here we give a simple example of integrating SCR into an application 
to write checkpoints. Further sections in the user guide give more
details and demonstrate how to perform restart with SCR.
You can also look at the source of the :code:`test_api` program and
other programs in the examples directory.

.. code-block:: c

  int main(int argc, char* argv[]) {
    MPI_Init(argc, argv);
    
    /* Call SCR_Init after MPI_Init */
    SCR_Init();
  
    for(int t = 0; t < TIMESTEPS; t++)
    {
      /* ... Do work ... */
  
      int flag;
      /* Ask SCR if we should take a checkpoint now */
      SCR_Need_checkpoint(&flag);
      if (flag)
        checkpoint();
    }
  
    /* Call SCR_Finalize before MPI_Finalize */
    SCR_Finalize();
    MPI_Finalize();
    return 0;
  }
  
  void checkpoint() {
    /* Tell SCR that you are getting ready to start a checkpoint phase */
    SCR_Start_checkpoint();
  
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  
    char file[256];
    /* create your checkpoint file name */
    sprintf(file, "rank_%d.ckpt", rank);
  
    /* Call SCR_Route_file to request a new file name (scr_file) that will cause
       your application to write the file to a fast tier of storage, e.g.,
       a burst buffer */
    char scr_file[SCR_MAX_FILENAME];
    SCR_Route_file(file, scr_file);
  
    /* Use the new file name to perform your checkpoint I/O */
    FILE* fs = fopen(scr_file, "w");
    if (fs != NULL) {
      fwrite(state, ..., fs);
      fclose(fs);
    }
  
    /* Tell SCR that you are done with your checkpoint phase */
    SCR_Complete_checkpoint(1);
    return;
  }

Final Thoughts
--------------

This was a really quick introduction to building and running
with SCR. For more information, please look at the more
detailed sections in the rest of this user guide or contact
us with questions.
