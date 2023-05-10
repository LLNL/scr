.. _sec-integration:

Integrate SCR
=============

This section provides details on how to integrate the SCR API into an application.
There are three steps to consider: Init/Finalize, Checkpoint, and Restart.
It is recommended to restart using the SCR Restart API, but it is not required.
Sections below describe each case.
Additionally, there is a section describing how to configure SCR
based on application settings.

The presentation here is a good way in practice to integrate SCR in steps.
First, one can add calls to Init and Finalize and stop to check that the
application successfully compiles, links, and runs with the SCR library.
One can run the application with :code:`SCR_DEBUG=1` set to verify that :code:`SCR_Init` is being called.
Second, one can then add Output API calls and stop to verify
that the application properly writes its checkpoints with SCR.
Third, one can then add Restart API calls and stop to verify
that the application successfully reads its checkpoints through SCR.
Finally, one can add any necessary calls to configure SCR based on application options.

Using the SCR API
-----------------

Before adding calls to the SCR library,
consider that an application has existing checkpointing code that looks like the following:

.. code-block:: c

  int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    /* initialize our state from checkpoint file */
    state = restart();

    for (int t = 0; t < TIMESTEPS; t++) {
      /* ... do work ... */

      /* every so often, write a checkpoint */
      if (t % CHECKPOINT_FREQUENCY == 0)
        checkpoint(t);
    }

    MPI_Finalize();
    return 0;
  }

  void checkpoint(int timestep) {
    /* rank 0 creates a directory on the file system,
     * and then each process saves its state to a file */

    /* define checkpoint directory for the timestep */
    char checkpoint_dir[256];
    sprintf(checkpoint_dir, "timestep.%d", timestep);

    /* get rank of this process */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* rank 0 creates directory on parallel file system */
    if (rank == 0) mkdir(checkpoint_dir);

    /* hold all processes until directory is created */
    MPI_Barrier(MPI_COMM_WORLD);

    /* build file name of checkpoint file for this rank */
    char checkpoint_file[256];
    sprintf(checkpoint_file, "%s/rank_%d.ckpt",
      checkpoint_dir, rank
    );

    /* each rank opens, writes, and closes its file */
    FILE* fs = fopen(checkpoint_file, "w");
    if (fs != NULL) {
      fwrite(checkpoint_data, ..., fs);
      fclose(fs);
    }

    /* wait for all files to be closed */
    MPI_Barrier(MPI_COMM_WORLD);

    /* rank 0 updates the pointer to the latest checkpoint */
    FILE* fs = fopen("latest", "w");
    if (fs != NULL) {
      fwrite(checkpoint_dir, ..., fs);
      fclose(fs);
    }
  }

  void* restart() {
    /* rank 0 broadcasts directory name to read from,
     * and then each process reads its state from a file */

    /* get rank of this process */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /* rank 0 reads and broadcasts checkpoint directory name */
    char checkpoint_dir[256];
    if (rank == 0) {
      FILE* fs = fopen("latest", "r");
      if (fs != NULL) {
        fread(checkpoint_dir, ..., fs);
        fclose(fs);
      }
    }
    MPI_Bcast(checkpoint_dir, sizeof(checkpoint_dir), MPI_CHAR, ...);

    /* build file name of checkpoint file for this rank */
    char checkpoint_file[256];
    sprintf(checkpoint_file, "%s/rank_%d.ckpt",
      checkpoint_dir, rank
    );

    /* each rank opens, reads, and closes its file */
    FILE* fs = fopen(checkpoint_file, "r");
    if (fs != NULL) {
      fread(state, ..., fs);
      fclose(fs);
    }

    return state;
  }

The following code exemplifies the changes necessary to integrate SCR.
Each change is numbered for further discussion below.

Init/Finalize
^^^^^^^^^^^^^

You must add calls to :code:`SCR_Init` and :code:`SCR_Finalize`
in order to start up and shut down the library.
The SCR library uses MPI internally,
and all calls to SCR must be from within a well defined MPI environment,
i.e., between :code:`MPI_Init` and :code:`MPI_Finalize`.

For example, one can modify the source to look something like this:

.. code-block:: c

  /**** change #0 ****/
  #include "scr.h"

  int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    /* add optional calls to SCR_Config() to configure SCR
     * based on application command line options */
    //SCR_Configf("SCR_PREFIX=%s", ...);

    /**** change #1 ****/
    SCR_Init();

    state = restart();

    for (int t = 0; t < TIMESTEPS; t++) {
      /* ... do work ... */

      /**** change #2 ****/
      int need_checkpoint;
      SCR_Need_checkpoint(&need_checkpoint);
      if (need_checkpoint)
        checkpoint(t);

      /**** change #3 ****/
      int should_exit;
      SCR_Should_exit(&should_exit);
      if (should_exit)
        break;
    }

    /**** change #4 ****/
    SCR_Finalize();

    MPI_Finalize();
    return 0;
  }

First, as noted in change #0,
include the SCR header in any source file where SCR calls are added.

As shown in change #1,
one must call :code:`SCR_Init()` to initialize the SCR library before it can be used.
SCR uses MPI, so SCR must be initialized after MPI has been initialized.
Internally, SCR duplicates :code:`MPI_COMM_WORLD` during :code:`SCR_Init`,
so MPI messages from the SCR library do not mix with messages sent by the application.

Additionally, one may configure SCR with calls to :code:`SCR_Config`.
Any calls to :code:`SCR_Config` must come before :code:`SCR_Init`.
Because it is common to configure SCR based on application command line options provided by the user,
it is typical to call :code:`SCR_Init` after application command line processing.
For some common examples with :code:`SCR_Config`, see :ref:`sec-integration-config`.

Then, as shown in change #4,
one should shut down the SCR library by calling :code:`SCR_Finalize()`.
This must be done before calling :code:`MPI_Finalize()`.
Some applications contain multiple calls to :code:`MPI_Finalize`.
In such cases, be sure to account for each call.
It is important to call :code:`SCR_Finalize`,
because SCR flushes any cached dataset to the prefix directory at this point.

As shown in change #2,
the application may rely on SCR to determine when to
checkpoint by calling :code:`SCR_Need_checkpoint()`.
SCR can be configured with information on failure rates and checkpoint costs
for the particular host platform, so this function provides a portable
method to guide an application toward an optimal checkpoint frequency.
For this, the application should call :code:`SCR_Need_checkpoint`
at each opportunity that it could checkpoint, e.g., at the end of each time step,
and then initiate a checkpoint when SCR advises it to do so.
An application may ignore the output of :code:`SCR_Need_checkpoint`,
and it does not have to call the function at all.
The intent of :code:`SCR_Need_checkpoint` is to provide a portable way for
an application to determine when to checkpoint across platforms with different
reliability characteristics and different file system speeds.

Also note how the application can call :code:`SCR_Should_exit`
to determine whether it is time to stop as shown in change #3.
This is important so that an application stops with sufficient
time remaining to copy datasets from cache to the parallel file system
before the allocation expires.
It is recommended to call this function after completing a checkpoint.

Checkpoint
^^^^^^^^^^

To actually write a checkpoint, there are three steps.
First, the application must call :code:`SCR_Start_output` with the :code:`SCR_FLAG_CHECKPOINT` flag
to define the start boundary of a new checkpoint.
It must do this before it creates any file belonging to the new checkpoint.
Then, the application must call :code:`SCR_Route_file` for each file
that it will write in order to register the file with SCR and to
acquire the full path to be used to open the file.
Finally, it must call :code:`SCR_Complete_output`
to define the end boundary of the checkpoint.

Every process must call :code:`SCR_Start_output` and :code:`SCR_Complete_output`,
even if the process does not write any files during the checkpoint.
These two functions are collective over all processes in :code:`MPI_COMM_WORLD`.
Only processes that write files need to call :code:`SCR_Route_file`.
All files registered through a call to :code:`SCR_Route_file` between a given
:code:`SCR_Start_output` and :code:`SCR_Complete_output` pair are considered to
be part of the same checkpoint file set.

Some example SCR checkpoint code looks like the following:

.. code-block:: c

  void checkpoint(int timestep) {
    /* each process saves its state to a file */

    /**** change #5 ****/
    char ckpt_name[SCR_MAX_FILENAME];
    snprintf(ckpt_name, sizeof(ckpt_name), "timestep.%d", timestep);
    SCR_Start_output(ckpt_name, SCR_FLAG_CHECKPOINT);

    /* define checkpoint directory for the timestep */
    char checkpoint_dir[256];
    sprintf(checkpoint_dir, "timestep.%d", timestep);

    /* get rank of this process */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    /**** change #6 ****/
    /*
        if (rank == 0)
          mkdir(checkpoint_dir);

        // hold all processes until directory is created
        MPI_Barrier(MPI_COMM_WORLD);
    */

    /* build file name of checkpoint file for this rank */
    char checkpoint_file[256];
    sprintf(checkpoint_file, "%s/rank_%d.ckpt",
      checkpoint_dir, rank
    );

    /**** change #7 ****/
    char scr_file[SCR_MAX_FILENAME];
    SCR_Route_file(checkpoint_file, scr_file);

    /**** change #8 ****/
    /* each rank opens, writes, and closes its file */
    int valid = 1;
    FILE* fs = fopen(scr_file, "w");
    if (fs != NULL) {
      int write_rc = fwrite(checkpoint_data, ..., fs);
      if (write_rc == 0) {
        /* failed to write file, mark checkpoint as invalid */
        valid = 0;
      }
      fclose(fs);
    } else {
      /* failed to open file, mark checkpoint as invalid */
      valid = 0;
    }

    /**** change #9 ****/
    /*
        // wait for all files to be closed
        MPI_Barrier(MPI_COMM_WORLD);

        // rank 0 updates the pointer to the latest checkpoint
        FILE* fs = fopen("latest", "w");
        if (fs != NULL) {
          fwrite(checkpoint_dir, ..., fs);
          fclose(fs);
        }
    */

    /**** change #10 ****/
    SCR_Complete_output(valid);
  }

As shown in change #5, the application must inform SCR when it is starting a new checkpoint
by calling :code:`SCR_Start_output()` with the :code:`SCR_FLAG_CHECKPOINT`.
The application should provide a name for the checkpoint,
and all processes must provide the same name and the same flags values.
In this example, the application timestep is used to name the checkpoint.
For applications that create a directory to hold all files of a checkpoint,
the name of the directory often serves as a good value for the SCR checkpoint name.

The application must inform SCR when it has completed the checkpoint
with a corresponding call to :code:`SCR_Complete_output()`
as shown in change #10.
When calling :code:`SCR_Complete_output()`, each process sets the :code:`valid` flag to indicate
whether it wrote all of its checkpoint files successfully.
Note how a :code:`valid` variable has been added to track any errors while writing the checkpoint.

SCR manages checkpoint directories,
so the :code:`mkdir` operation is removed in change #6.
Additionally, the application can rely on SCR to track the latest checkpoint,
so the logic to track the latest checkpoint is removed in change #9.

Between the call to :code:`SCR_Start_output()` and :code:`SCR_Complete_output()`,
the application must register each of its checkpoint files by calling
:code:`SCR_Route_file()` as shown in change #7.
As input, the process may provide either an absolute or relative path to its checkpoint file.
If given a relative path, SCR internally prepends the current working directory to the path when :code:`SCR_Route_file()` is called.
In either case, the fully resolved path must be located somewhere within the prefix directory.
If SCR copies the file to the parallel file system, it writes the file to this path.
When storing the file in cache, SCR "routes" the file by replacing any leading directory
on the file name with a path that points to a cache directory.
SCR returns this routed path as output.

As shown in change #8,
the application must use the exact string returned by :code:`SCR_Route_file()` to open
its checkpoint file.

Restart with SCR
^^^^^^^^^^^^^^^^

To use SCR for restart, the application must call :code:`SCR_Have_restart`
to determine whether SCR has a previous checkpoint loaded.
If there is a checkpoint available, the application
can call :code:`SCR_Start_restart` to tell SCR that it is initiating a restart operation.

The application must call :code:`SCR_Route_file` to acquire the
full path to each file that it will read during the restart.
The calling process can specify either an absolute or relative path in its input file name.
If given a relative path, SCR internally prepends the current working directory when :code:`SCR_Route_file()` is called.
The fully resolved path must be located somewhere within the prefix directory and it must correspond
to a file associated with the particular checkpoint name that SCR returned in :code:`SCR_Start_restart`.

After the application reads its checkpoint files, it must call
:code:`SCR_Complete_restart` to indicate that it has completed reading its checkpoint files.
If any process fails to read its checkpoint files,
:code:`SCR_Complete_restart` returns something other than :code:`SCR_SUCCESS` on all processes
and SCR prepares the next most recent checkpoint if one is available.
The application can try again with another call to :code:`SCR_Have_restart`.

For backwards compatibility, the application can provide just a file name in :code:`SCR_Route_file`
during restart, even if the combination of the current working directory and the provided file name
do not specify the correct path on the parallel file system.
This usage is deprecated, and it may be not be supported in future releases.
Instead it is recommended that one construct the full path to the checkpoint file
using information from the checkpoint name returned by :code:`SCR_Start_restart`.

Some example SCR restart code may look like the following:

.. code-block:: c

  void* restart() {
    /* each process reads its state from a file */

    /**** change #12 ****/
    int restarted = 0;
    while (! restarted) {

      /**** change #13 ****/
      int have_restart = 0;
      char ckpt_name[SCR_MAX_FILENAME];
      SCR_Have_restart(&have_restart, ckpt_name);
      if (! have_restart) {
        /* no checkpoint available from which to restart */
        break;
      }

      /**** change #14 ****/
      SCR_Start_restart(checkpoint_dir);

      /* get rank of this process */
      int rank;
      MPI_Comm_rank(MPI_COMM_WORLD, &rank);

      /**** change #15 ****/
      /*
          // rank 0 reads and broadcasts checkpoint directory name
          char checkpoint_dir[256];
          if (rank == 0) {
            FILE* fs = fopen("latest", "r");
            if (fs != NULL) {
              fread(checkpoint_dir, ..., fs);
              fclose(fs);
            }
          }
          MPI_Bcast(checkpoint_dir, sizeof(checkpoint_dir), MPI_CHAR, ...);
      */

      /**** change #16 ****/
      /* build path of checkpoint file for this rank given the checkpoint name */
      char checkpoint_file[256];
      sprintf(checkpoint_file, "%s/rank_%d.ckpt",
        ckpt_name, rank
      );

      /**** change #17 ****/
      char scr_file[SCR_MAX_FILENAME];
      SCR_Route_file(checkpoint_file, scr_file);

      /**** change #18 ****/
      /* each rank opens, reads, and closes its file */
      int valid = 1;
      FILE* fs = fopen(scr_file, "r");
      if (fs != NULL) {
        int read_rc = fread(state, ..., fs);
        if (read_rc == 0) {
          /* failed to read file, mark restart as invalid */
          valid = 0;
        }
        fclose(fs);
      } else {
        /* failed to open file, mark restart as invalid */
        valid = 0;
      }

      /**** change #19 ****/
      int rc = SCR_Complete_restart(valid);

      /**** change #20 ****/
      restarted = (rc == SCR_SUCCESS);
    }

    if (restarted) {
      return state;
    } else {
      return new_run_state;
    }
  }

With SCR, the application can attempt to restart from its most recent checkpoint,
and if that fails, SCR loads the next most recent checkpoint.
This process continues until the application successfully restarts or exhausts
all available checkpoints.
To enable this, we create a loop around the restart process, as shown in change #12.

For each attempt, the application must first call :code:`SCR_Have_restart()` to
determine whether SCR has a checkpoint available as shown in change #13.
If there is a checkpoint,
the application calls :code:`SCR_Start_restart()` as shown in change #14 to inform SCR that it is beginning its restart.
The application logic to identify the latest checkpoint is removed in change #15,
since SCR manages which checkpoint to load.
The application should use the checkpoint name returned in :code:`SCR_Start_restart()`
to construct the path to its checkpoint file as shown in change #16.
In this case, the checkpoint name is the same as the checkpoint directory,
so the path to the file is easy to compute.
The application obtains the path to its checkpoint file
by calling :code:`SCR_Route_file()` in change #17.
It uses this path to open the file for reading in change #18.
After the process reads each of its checkpoint files,
it informs SCR that it has completed reading its data with a call
to :code:`SCR_Complete_restart()` in change #19.

When calling :code:`SCR_Complete_restart()`, each process sets the :code:`valid` flag to indicate
whether it read all of its checkpoint files successfully.
Note how a :code:`valid` variable has been added to track whether the process successfully reads its checkpoint.

As shown in change #20, SCR returns :code:`SCR_SUCCESS` from :code:`SCR_Complete_restart()` if all processes succeeded.
If the return code is something other than :code:`SCR_SUCCESS`, then at least one process failed to restart.
In that case, SCR loads the next most recent checkpoint if one is available,
and the application can call :code:`SCR_Have_restart()` to iterate through the process again.

It is not required for an application to loop on failed restarts, but SCR allows for that.
SCR never loads a checkpoint that is known to be incomplete or one that is explicitly marked as invalid,
though it is still possible the application will encounter an error while reading those files on restart.
If an application fails to restart from a checkpoint, SCR marks that checkpoint as invalid
so that it will not attempt to load that checkpoint again in future runs.

It is possible to use the SCR Restart API even if the application must restart from a global file system.
For such applications, one should set :code:`SCR_GLOBAL_RESTART=1`.
Under this mode, SCR flushes any cached checkpoint to the prefix directory during :code:`SCR_Init`,
and it configures its restart operation to use cache bypass mode so that :code:`SCR_Route_file`
directs the application to read its files directly from the parallel file system.

.. _sec-integration-restart-without:

Restart without SCR
^^^^^^^^^^^^^^^^^^^

If the application does not use SCR for restart,
it should not make calls to :code:`SCR_Have_restart`,
:code:`SCR_Start_restart`, :code:`SCR_Route_file`, or
:code:`SCR_Complete_restart` during the restart.
Instead, it should access files directly from the parallel file system.

When not using SCR for restart, one should set :code:`SCR_FLUSH_ON_RESTART=1`,
which causes SCR to flush any cached checkpoint to the file system during :code:`SCR_Init`.
Additionally, one should set :code:`SCR_FETCH=0` to disable SCR from loading a checkpoint during :code:`SCR_Init`.
The application can then read its checkpoint from the parallel file system after calling :code:`SCR_Init`.

If the application reads a checkpoint that it previously wrote through SCR,
it should call :code:`SCR_Current` after :code:`SCR_Init` to notify SCR which checkpoint that it restarted from.
This lets SCR configure its internal state to properly track the ordering of new datasets that the application writes.

If restarting without SCR and if :code:`SCR_Current` is not called,
the value of the :code:`SCR_FLUSH` counter will not be preserved between restarts.
The counter will be reset to its upper limit with each restart.
Thus each restart may introduce some offset in a sequence of periodic SCR flushes.

.. _sec-integration-config:

Configure SCR for application settings
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Applications often provide their users with command line options
or configuration files whose settings need to affect how SCR behaves.
For this, one can call :code:`SCR_Config` to configure SCR before calling :code:`SCR_Init`.

For example, it is common for applications to provide an :code:`--output <dir>` option
that sets the directory in which datasets are written.
One typically must set :code:`SCR_PREFIX` to that same path::

  SCR_Configf("SCR_PREFIX=%s", dir);

Many applications provide at least two restart modes:
one in which the application restarts from its most recent checkpoint,
and one in which the user names a specific checkpoint.
To restart from the most recent checkpoint,
one can just rely on the normal SCR behavior,
since SCR restarts from the most recent checkpoint by default.
In the case that a specific checkpoint is named,
one can set :code:`SCR_CURRENT` to the appropriate dataset name::

  SCR_Configf("SCR_CURRENT=%s", ckptname);

Some applications provide users with options that determine
file access patterns and the size of output datasets.
For those, it may be useful to call :code:`SCR_Config` to set parameters such as
:code:`SCR_CACHE_BYPASS`, :code:`SCR_GLOBAL_RESTART`, and :code:`SCR_CACHE_SIZE`.

A number of common configuration settings are listed in :ref:`sec-config-common`.

Building with the SCR library
-----------------------------

To compile and link with the SCR library,
add the flags shown below to your compile and link lines.
The value of the variable :code:`SCR_INSTALL_DIR` should be the path
to the installation directory for SCR.

========================== ============================================================================
Compile Flags              :code:`-I$(SCR_INSTALL_DIR)/include`
C Dynamic Link Flags       :code:`-L$(SCR_INSTALL_DIR)/lib64 -lscr -Wl,-rpath,$(SCR_INSTALL_DIR)/lib64`
C Static Link Flags        :code:`-L$(SCR_INSTALL_DIR)/lib64 -lscr`
Fortran Dynamic Link Flags :code:`-L$(SCR_INSTALL_DIR)/lib64 -lscrf -Wl,-rpath,$(SCR_INSTALL_DIR)/lib64`
Fortran Static Link Flags  :code:`-L$(SCR_INSTALL_DIR)/lib64 -lscrf`
========================== ============================================================================

.. note::
   On some platforms the default library installation path will be :code:`/lib` instead of :code:`/lib64`.

If Spack was used to build SCR, the :code:`SCR_INSTALL_DIR` can be found with:

.. code-block:: bash

  spack location -i scr

For applications built with CMake,
SCR provides an :code:`scrConfig.cmake` package configuration file
that defines C and Fortran targets for its shared and static libraries.

====================== ============================================================================
C Shared Library       :code:`scr::scr`
C Static Library       :code:`scr::scr-static`
Fortran Shared Library :code:`scr::scrf`
Fortran Static Library :code:`scr::scrf-static`
====================== ============================================================================

These targets define the compile and link flags necessary for SCR and its dependencies.
For example, to compile and link to the SCR shared library,
the :code:`CMakeLists.txt` of a C application can use statements like:

.. code-block:: cmake

  FIND_PACKAGE(scr REQUIRED)
  ADD_EXECUTABLE(myapp myapp.c)
  TARGET_LINK_LIBRARIES(myapp PRIVATE scr::scr)

The SCR package configuration file is installed to :code:`${SCR_INSTALL_DIR}/share/scr/cmake`.
One should include this path in the CMake prefix search path, e.g.,:

.. code-block:: bash

  export CMAKE_PREFIX_PATH=${SCR_INSTALL_DIR}
