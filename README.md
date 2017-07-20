# Scalable Checkpoint / Restart (SCR) Library

The Scalable Checkpoint / Restart (SCR) library enables MPI applications
to utilize distributed storage on Linux clusters to attain high file I/O
bandwidth for checkpointing and restarting large-scale jobs. With SCR,
jobs run more efficiently, recompute less work upon a failure, and reduce
load on critical shared resources such as the parallel file system.

Detailed usage is provided in the [user manual](/doc/scr_users_manual.pdf).

## Build and install

Use the standard, configure/make/make install to configure, build, and
install the library, e.g.:

    ./configure \
      --prefix=/usr/local/tools/scr-1.1 \
      --with-scr-config-file=/etc/scr.conf
    make
    make install

To uninstall the installation:

    make uninstall

SCR is layered on top of MPI, and it must be built with the MPI library
used by the application.  It uses the standard mpicc compiler wrapper.
If there are multiple MPI libraries, a separate SCR library must be
built for each MPI library.

If you are using the scr.conf file, you'll also need to edit this file
to match the settings on your system.  Please open this file and make
any necessary changes -- it is self-documented with comments.  After
modifying this file, copy it to the location specified in your configure
step.  The configure option simply informs the SCR install where to
look for the file, it does not modify or install the file.

## Authors

Numerous people have [contributed](https://github.com/llnl/scr/graphs/contributors) to the SCR project.

To reference SCR in a publication, please cite the following paper:

* Adam Moody, Greg Bronevetsky, Kathryn Mohror, Bronis R. de Supinski, [Design, Modeling, and Evaluation of a Scalable Multi-level Checkpointing System](http://dl.acm.org/citation.cfm?id=1884666), LLNL-CONF-427742, Supercomputing 2010, New Orleans, LA, November 2010.

Additional information and research publications can be found here:

[http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi](http://computation.llnl.gov/projects/scalable-checkpoint-restart-for-mpi)
