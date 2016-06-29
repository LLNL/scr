========================================================================
Scalable Checkpoint / Restart (SCR) Library
========================================================================

The Scalable Checkpoint / Restart (SCR) library enables MPI applications
to utilize distributed storage on Linux clusters to attain high file I/O
bandwidth for checkpointing and restarting large-scale jobs. With SCR,
jobs run more efficiently, recompute less work upon a failure, and reduce
load on critical shared resources such as the parallel file system.

Detailed usage is provided in the [user manual](/doc/scr_users_manual.pdf).

-----------------------------
Dependencies
-----------------------------

SCR depends on a number of other software packages.

Required:

  * [pdsh](http://sourceforge.net/projects/pdsh) -- parallel remote shell command (pdsh, dshbak)
    e.g.:
    ```bash
    wget http://downloads.sourceforge.net/project/pdsh/pdsh/pdsh-2.18/pdsh-2.18.tar.bz2
    bunzip2 pdsh-2.18.tar.bz2
    tar -xf pdsh-2.18.tar
    pushd pdsh-2.18
    ./configure --prefix=/tmp
    make
    make install
    ```

  * [Date::Manip](http://search.cpan.org/~sbeck/Date-Manip-5.54/lib/Date/Manip.pod) -- Perl module for date/time interpretation

  * [SLURM](http://slurm.schedmd.com/) -- resource manager

Optional:

  * libyogrt -- your one get remaining time library
    * Not currently available outside of LLNL.
      Enables a running job to determine how much time it has left
      in its resource allocation.

  * [io-watchdog](https://github.com/grondo/io-watchdog) -- tool to catch and terminate hanging jobs
    * Spawns a thread that can kill an MPI process (and hence
      the job) when it detects that the process has not written
      data is some specified amount of time.  This is very useful
      to catch and kill hanging jobs, so that SCR can restart the
      job or fetch the latest checkpoint.

  * [MySQL](http://www.mysql.com) -- for logging events

SCR is designed to be ported to other resource managers, so with some
work, one may replace SLURM with something else.  Also libyogrt is
optional and similarly can be replaced with something else.

-----------------------------
Configuration
-----------------------------

Most of the default settings in the SCR library can be adjusted.  One
may customize default settings via configure options, compile-time
defines, or a system configuration file.

In particular, the following parameters likely need to be adjusted for
other platforms:

### Cache and control directories:

It is critical to configure SCR with directory paths it should use
for cache and control directories.  Each of these directories should
correspond to a path leading to a storage device that is local to the
compute node.  For more information, see the SCR directories section
below.

### scr.conf:

A number of options can and must be specified for an SCR installation.
A convenient method to set these parameters is to create a system
configuration file.  To help you get started, an example config file
is provided in scr.conf.  Copy this file, customize it with your
desired settings, and configure your SCR build to know where to look
for it using the --with-scr-config-file configure option, e.g.,

    configure --with-scr-config-file=/etc/scr.conf ...

### libyogrt support:

If libyogrt is available, specify --with-yogrt[=PATH] during the
configure.

The function of libyogrt is to enable a job to determine how much
time it has left in its current allocation.  While libyogrt is not
available outside of LLNL, other computer centers often have some
mechanism that provides similar functionality.  It should be straigt-
forward to replace calls to libyogrt with something else.  One must
simply modify the scr_env_seconds_remaining() implementation in
src/scr_env.c to return the number of seconds left in the job
allocation.

### MySQL DB Logging:

SCR can log events to a MySQL database.  However, this feature is
still in development.  To enable logging to MySQL, configure with
--with-mysql. If you would like to give it a try, see the
scr.mysql file for further instructions.

The compile-time define variables are specified in src/scr_conf.h.  One
may modify the settings in this file before building to set different
defaults.  In many cases, one may be able to avoid having to create a
system configuration using this method.

-----------------------------
Build and install
-----------------------------

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

-----------------------------
SCR directories
-----------------------------

There are three types of directories where SCR manages files.

### (1) Cache directory

SCR instructs the application to write its checkpoint files to a cache
directory, and SCR computes and stores redundancy data here.  SCR can
be configured to use more than one cache directory in a single run.
The cache directory must be local to the compute node.

The cache directory must be large enough to hold at least one full
application checkpoint.  However, it is better if the cache is large
enough to store at least two checkpoints.  When using storage local
to the compute node, the cache must be roughly 0.5x-3x the size of
the node's memory to be effective.

The library is hard-coded to use /tmp as the cache directory.  To
change this location, do any of the following:

  * change the SCR_CACHE_BASE value in src/scr_conf.h,

  * specify the new path during configure using the
     --with-scr-cache-base=PATH option,

  * or set the value in a system configuration file.

To enable multiple cache directories, one must use a system
configuration file.

### (2) Control directory

SCR stores files to track its internal state in the control directory.
Currently, this directory must be local to the compute node.  Files
written to the control directory are small and they are read and written
to frequently, so RAM disk works well for this.  There are a variety of
files kept in the control directory.  For some of these files, all
processes maintain their own file, and for others, only rank 0 accesses
the file.

The library is hard-coded to use /tmp as the control directory.  To
change this location, do any of the following:

  * change the SCR_CNTL_BASE value in src/scr_conf.h,

  * specify the new path during configure using the
     --with-scr-cntl-base=PATH option,

  * or set the value in a system configuration file.

### (3) Prefix directory - on parallel file system

This is the directory where SCR will fetch and flush checkpoint sets
to the parallel file system.

The prefix directory defaults to the current working directory of the
application.  To override this, the user can set the SCR_PREFIX
parameter at run time.

-----------------------------
RAM disk
-----------------------------

On systems where compute nodes are diskless, often RAM disk is the only
option for a checkpoint cache.  When using RAM disk, one should
configure the Linux hard limit to be around two-thirds of the amount of memory
on a node.  This is a high limit, however Linux only allocates space as
needed, so it is safe to use a high setting.  Jobs which do not use RAM
disk will not be negatively affected by using a high setting.  This
setting must be set by the system administrators.

-----------------------------
Nodeset format
-----------------------------

Several of the commands process the nodeset allocated to the job.  The
node names are expected to be of the form:

    clustername#

where clustername is a string of letters common for all nodes,
and # is the node number, like atlas35 and atlas173.

More generally:

<any non digit characters><any digit characters>

so things like atlas-31, atlas31, or even $$##@$!21 are all valid.

What is currently not valid is something like:

atlas-31vm1

because this breaks the regular expressinos since it only expects digits
characters after the non digit characters.  This limitation may be further 
generalized in the future.

The scr_hostlist.pm perl module compresses lists of such nodenames into a
form like so:

    atlas[31-43,45,48-51]
