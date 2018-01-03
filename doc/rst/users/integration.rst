.. _sec-integration:

Integrate SCR
=============

This section provides details on how to integrate the SCR API into an application.

Using the SCR API
-----------------

Before adding calls to the SCR library,
consider that an application has existing checkpointing code that looks like the following

.. code-block:: c

  int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
  
    /* initialize our state from checkpoint file */
    state = restart();
  
    for (t = 0; t < TIMESTEPS; t++) {
      /* ... do work ... */
  
      /* every so often, write a checkpoint */
      if (t % CHECKPOINT_FREQUENCY == 0)
        checkpoint();
    }
  
    MPI_Finalize();
    return 0;
  }
  
  void checkpoint() {
    /* rank 0 creates a directory on the file system,
     * and then each process saves its state to a file */
  
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

There are three steps to consider when integrating the SCR API into an application:
Init/Finalize, Checkpoint, and Restart.
One may employ the scalable checkpoint capability of SCR without the scalable restart capability.
While it is most valuable to utilize both, some applications cannot use the scalable restart.

The following code exemplifies the changes necessary to integrate SCR.
Each change is numbered for further discussion below.

Init/Finalize
^^^^^^^^^^^^^

You must add calls to :code:`SCR_Init` and :code:`SCR_Finalize`
in order to start up and shut down the library.
The SCR library uses MPI internally,
and all calls to SCR must be from within a well defined MPI environment,
i.e., between :code:`MPI_Init` and :code:`MPI_Finalize`.
It is recommended to call :code:`SCR_Init` immediately after :code:`MPI_Init`
and to call :code:`SCR_Finalize` just before :code:`MPI_Finalize`.
For example, modify the source to look something like this

.. code-block:: c

  int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
  
    /**** change #1 ****/
    SCR_Init();
  
    /**** change #2 ****/
    int have_restart;
    SCR_Have_restart(&have_restart, NULL);
    if (have_restart)
      state = restart();
    else
      state = new_run_state;
  
    for (t = 0; t < TIMESTEPS; t++) {
      /* ... do work ... */
  
      /**** change #3 ****/
      int need_checkpoint;
      SCR_Need_checkpoint(&need_checkpoint);
      if (need_checkpoint)
        checkpoint();
    }
  
    /**** change #4 ****/
    SCR_Finalize();
  
    MPI_Finalize();
    return 0;
  }

First, as shown in change #1,
one must call :code:`SCR_Init()` to initialize the SCR library before it can be used.
SCR uses MPI, so SCR must be initialized after MPI has been initialized.
Similarly, as shown in change #4,
one should shut down the SCR library by calling :code:`SCR_Finalize()`.
This must be done before calling :code:`MPI_Finalize()`.
Internally, SCR duplicates :code:`MPI_COMM_WORLD` during :code:`SCR_Init`,
so MPI messages from the SCR library do not mix with messages sent by the application.

Some applications contain multiple calls to :code:`MPI_Finalize`.
In such cases, be sure to account for each call.
The same applies to :code:`MPI_Init` if there are multiple calls to this function.

In change #2, the application can call :code:`SCR_Have_restart()` to determine
whether there is a checkpoint to read in.
If so, it calls its restart function, otherwise it assumes it is starting from scratch.
This should only be called if the application is using the scalable restart feature of SCR.

As shown in change #3,
the application may rely on SCR to determine when to
checkpoint by calling :code:`SCR_Need_checkpoint()`.
SCR can be configured with information on failure rates and checkpoint costs
for the particular host platform, so this function provides a portable
method to guide an application toward an optimal checkpoint frequency.
For this, the application should call :code:`SCR_Need_checkpoint`
at each natural opportunity it has to checkpoint, e.g., at the end of each time step,
and then initiate a checkpoint when SCR advises it to do so.
An application may ignore the output of :code:`SCR_Need_checkpoint`,
and it does not have to call the function at all.
The intent of :code:`SCR_Need_checkpoint` is to provide a portable way for
an application to determine when to checkpoint across platforms with different
reliability characteristics and different file system speeds.

Checkpoint
^^^^^^^^^^

To actually write a checkpoint, there are three steps.
First, the application must call :code:`SCR_Start_checkpoint`
to define the start boundary of a new checkpoint.
It must do this before it opens any file belonging to the new checkpoint.
Then, the application must call :code:`SCR_Route_file` for each file
that it will write in order to register the file with SCR and to
determine the full path and file name to open each file.
Finally, it must call :code:`SCR_Complete_checkpoint`
to define the end boundary of the checkpoint.

If a process does not write any files during a checkpoint,
it must still call :code:`SCR_Start_checkpoint` and :code:`SCR_Complete_checkpoint`
as these functions are collective.
All files registered through a call to :code:`SCR_Route_file` between a given
:code:`SCR_Start_checkpoint` and :code:`SCR_Complete_checkpoint` pair are considered to
be part of the same checkpoint file set.
Some example SCR checkpoint code looks like the following

.. code-block:: c

  void checkpoint() {
    /* each process saves its state to a file */
  
    /**** change #5 ****/
    SCR_Start_checkpoint();
  
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
    FILE* fs = fopen(scr_file, "w");
    if (fs != NULL) {
      fwrite(checkpoint_data, ..., fs);
      fclose(fs);
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
    SCR_Complete_checkpoint(valid);
  
    /**** change #11 ****/
    /* Check whether we should stop */
    int should_exit;
    SCR_Should_exit(&should_exit);
    if (should_exit) {
      exit(0);
    }
  }

As shown in change #5, the application must inform SCR when it is starting a new checkpoint
by calling :code:`SCR_Start_checkpoint()`.
Similarly, it must inform SCR when it has completed the checkpoint
with a corresponding call to :code:`SCR_Complete_checkpoint()`
as shown in change #10.
When calling :code:`SCR_Complete_checkpoint()`, each process sets the :code:`valid` flag to indicate
whether it wrote all of its checkpoint files successfully.

SCR manages checkpoint directories,
so the :code:`mkdir` operation is removed in change #6.
Additionally, the application can rely on SCR to track the latest checkpoint,
so the logic to track the latest checkpoint is removed in change #9.

Between the call to :code:`SCR_Start_checkpoint()` and :code:`SCR_Complete_checkpoint()`,
the application must register each of its checkpoint files by calling
:code:`SCR_Route_file()` as shown in change #7.
SCR "routes" the file by replacing any leading directory
on the file name with a path that points to another directory in which SCR caches data for the checkpoint.
As shown in change #8,
the application must use the exact string returned by :code:`SCR_Route_file()` to open
its checkpoint file.

Also note how the application can call :code:`SCR_Should_exit`
after a checkpoint to determine whether it is time to stop shown in change #11.
This is important so that an application stops with sufficient
time remaining to copy datasets from cache to the parallel file system
before the allocation expires.

Restart with SCR
^^^^^^^^^^^^^^^^

There are two options to access files during a restart: with and without SCR.
If an application is designed to restart such that each MPI task
only needs access to the files it wrote during the previous checkpoint,
then the application can utilize the scalable restart capability of SCR.
This enables the application to restart from a cached checkpoint in the existing resource allocation,
which saves the cost of writing to and reading from the parallel file system.

To use SCR for restart, the application  can call :code:`SCR_Have_restart`
to determine whether SCR has a previous checkpoint loaded.
If there is a checkpoint available, the application 
can call :code:`SCR_Start_restart` to tell SCR that a restart operation is beginning.
Then, the application must call :code:`SCR_Route_file` to determine the
full path and file name to each of its checkpoint files that it will read for restart.
The input file name to :code:`SCR_Route_file` does not need a path during restart,
as SCR will identify the file just based on its file name.
After the application reads in its checkpoint files, it must call 
:code:`SCR_Complete_restart` to indicate that it has completed reading its checkpoint files.
Some example SCR restart code may look like the following

.. code-block:: c

  void* restart() {
    /* each process reads its state from a file */
  
    /**** change #12 ****/
    SCR_Start_restart(NULL);
  
    /* get rank of this process */
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  
    /**** change #13 ****/
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
  
    /**** change #14 ****/
    /* build file name of checkpoint file for this rank */
    char checkpoint_file[256];
    sprintf(checkpoint_file, "rank_%d.ckpt",
      rank
    );
  
    /**** change #15 ****/
    char scr_file[SCR_MAX_FILENAME];
    SCR_Route_file(checkpoint_file, scr_file);
  
    /**** change #16 ****/
    /* each rank opens, reads, and closes its file */
    FILE* fs = fopen(scr_file, "r");
    if (fs != NULL) {
      fread(state, ..., fs);
      fclose(fs);
    }
  
    /**** change #17 ****/
    SCR_Complete_restart(valid);
  
    return state;
  }

As shown in change #12,
the application calls :code:`SCR_Start_restart()` to inform SCR that it is beginning its restart.
SCR automatically loads the most recent checkpoint,
so the application logic to identify the latest checkpoint is removed in change #13.
During a restart, the application only needs the file name,
so the checkpoint directory can be dropped from the path in change #14.
Instead, the application gets the path to use to open the checkpoint file
via a call to :code:`SCR_Route_file()` in change #15.
It then uses that path to open the file for reading in change #16.
After the process has read each of its checkpoint files,
it informs SCR that it has completed reading its data with a call
to :code:`SCR_Complete_restart()` in change #17.
When calling :code:`SCR_Complete_restart()`, each process sets the :code:`valid` flag to indicate
whether it read all of its checkpoint files successfully.

Restart without SCR
^^^^^^^^^^^^^^^^^^^

If the application does not use SCR for restart,
it should not make calls to :code:`SCR_Have_restart`,
:code:`SCR_Start_restart`, :code:`SCR_Route_file`, or 
:code:`SCR_Complete_restart` during the restart.
Instead, it should access files directly from the parallel file system.
When restarting without SCR,
the value of the :code:`SCR_FLUSH` counter will not be preserved between restarts.
The counter will be reset to its upper limit with each restart.
Thus, each restart may introduce some fixed offset in a series of periodic SCR flushes.
When not using SCR for restart, one should set the :code:`SCR_FLUSH_ON_RESTART` parameter to :code:`1`,
which will cause SCR to flush any cached checkpoint to the file system during :code:`SCR_Init`.

Building with the SCR library
-----------------------------

To compile and link with the SCR library,
add the flags in Table~\ref{table:build_flags} to your compile and link lines.
The value of the variable :code:`SCR_INSTALL_DIR` should be the path
to the installation directory for SCR.

SCR build flags

========================== ============================================================================
Compile Flags              :code:`-I$(SCR_INSTALL_DIR)/include`
C Dynamic Link Flags       :code:`-L$(SCR_INSTALL_DIR)/lib64 -lscr -Wl,-rpath,$(SCR_INSTALL_DIR)/lib64`
C Static Link Flags        :code:`-L$(SCR_INSTALL_DIR)/lib64 -lscr -lz`
Fortran Dynamic Link Flags :code:`-L$(SCR_INSTALL_DIR)/lib64 -lscrf -Wl,-rpath,$(SCR_INSTALL_DIR)/lib64`
Fortran Static Link Flags  :code:`-L$(SCR_INSTALL_DIR)/lib64 -lscrf -lz`
========================== ============================================================================
