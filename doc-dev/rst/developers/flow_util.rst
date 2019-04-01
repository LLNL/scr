.. _scripts:

Perl modules
------------

``scripts/common/scr_hostlist.pm``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Manipulates lists of hostnames. The elements in a set of hostnames are
expected to have a common alpha prefix (machine name) followed by a
number (node number). A hostlist can be specified in one of two forms:

============ ================================================================== ===========================
compressed   “atlas[3,5-7,9-11]”                                                Perl string scalar
uncompressed (“atlas3”,“atals5”,“atlas6”,“atlas7”,“atlas9”,“atlas10”,“atlas11”) Perl list of string scalars
============ ================================================================== ===========================

All functions in this module are global; no instance must be created.

Given a compressed hostlist, construct the corresponding uncompressed
hostlist (preserves order and duplicates).

::

     my @hostlist = scr_hostlist::expand($hostlist);

Given an uncompressed hostlist, construct a compressed hostlist
(preserves duplicate hostnames, but sorts list by node number).

::

     my $hostlist = scr_hostlist::compress(@hostlist);

Given references to two uncompressed hostlists, subtract second list
from first and return remainder as an uncompressed hostlist.

::

     my @hostlist = scr_hostlist::diff(\@hostlist1, \@hostlist2);

Given references to two uncompressed hostlists, return the intersection
of the two as an uncompressed hostlist.

::

     my @hostlist = scr_hostlist::intersect(\@hostlist1, \@hostlist2);

.. _scr_param_pm:

``scripts/common/scr_param.pm.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Reads and returns SCR configuration parameters, returning the first set
value found by searching in the following order:

#. Environment variable,

#. User configuration file,

#. System configuration file,

#. Default (build-time constant).

When an instance is created, it attempts to read the user configuration
file from ``SCR_CONF_FILE`` if that variable is set. Otherwise, it
attempts to read the user configuration file from ``<prefix>/.scrconf``,
where ``<prefix>`` is the prefix directory specified in ``SCR_PREFIX``
or the current working directory if ``SCR_PREFIX`` is not set.

Some parameters cannot be set by a user, and for these parameters any
settings in environment variables or the user configuration file are
ignored.

The majority of parameters return scalar values, but some return an
associated hash.

Allocate a new ``scr_param`` object.

::

     my $param = new scr_param();

Given the name of an SCR parameter, return its scalar value.

::

     my $val = $param->get($name);

Given the name of an SCR parameter, return a reference to its hash.

::

     my $hashref = $param->get_hash($name);

To disable a user from setting a parameter, add it to the ``no_user``
hash within the module implementation. Parameters listed in this hash
will not be affected by environment variables or user configuration file
settings.

Compile-time constants should be listed in the ``compile`` hash.

Utilities
---------

``scripts/common/scr_glob_hosts.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Uses ``scr_hostlist.pm`` to manipulate hostlists. Only accepts
compressed hostlists for input.

Given a compressed hostlist, return number of hosts.

::

     scr_glob_hosts --count "atlas[3,5-7,9-11]"

The example above returns “7”, as there are seven hosts specified in the
list.

Given a compressed hostlist, return the nth host.

::

     scr_glob_hosts --nth 3 "atlas[3,5-7,9-11]"

The example above returns “atlas6”, which is the third host.

Given two compressed hostlists, subtract one from the other and return
remainder.

::

     scr_glob_hosts --minus "atlas[3,5-7,9-11]":"atlas[5,7,20]"

The above example returns “atlas[3,6,9-11]”, which has removed “atlas5”
and “atlas7” from the first list.

Given two compressed hostlists, return intersection of the two.

::

     scr_glob_hosts --intersection "atlas[3,5-7,9-11]":"atlas[5,7,20]"

The above example returns “atlas[5,7]”, which is the list of common
hosts between the two lists.

``src/scr_flush_file.c``
~~~~~~~~~~~~~~~~~~~~~~~~

Utility to access info in SCR flush file. One must specify the prefix
directory from which to read the flush file.

Read the flush file and return the latest checkpoint id.

::

     scr_flush_file --latest /prefix/dir

The above command prints the checkpoint id of the most recent checkpoint
in the flush file. It exits with a return code of 0 if it found a
checkpoint id, and it exits with a return code of 1 otherwise.

Determine whether a specified checkpoint id needs to be flushed.

::

     scr_flush_file --needflush 6 /prefix/dir

The command above checks whether the ``SCR_FLUSH_KEY_LOCATION_PFS`` key
is set for the specified checkpoint id. If so, the command exits with 0,
otherwise is exits with 1.

List the location(s) containing a copy of the dataset.

::

     scr_flush_file --location 6 /prefix/dir

This command lists ``PFS`` for the parallel file system, and it lists
``CACHE`` for datasets stored in cache.

List the subdirectory where a dataset should be flushed to.

::

     scr_flush_file --subdir 6 /prefix/dir

``scripts/common/scr_list_dir.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Returns full path to control or cache directory. Uses ``scr_param.pm``.
This command should be executed in an environment where
``SCR_CONF_FILE`` is set to the same value as the running job.

#. Uses ``scr_param.pm`` to read ``SCR_CNTL_BASE`` to get base control
   directory.

#. Uses ``scr_param.pm`` to read ``CACHE`` hash from config file to get
   info on cache directories.

#. Invokes “``scr_env –user``” to get the username if not specified on
   command line.

#. Invokes “``scr_env –jobid``” to get the jobid if not specified on
   command line.

#. Combines base, user, and jobid to build and output full path to
   control or cache directory.

``scripts/TLCC/scr_list_down_nodes.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Runs a series of tests over all specified nodes and builds list of nodes
which fail one or more tests. Uses ``scr_hostlist.pm`` to manipulate
hostlists. Uses ``scr_param.pm`` to read various parameters.

#. Invokes “``scr_env –nodes``” to get the current nodeset, if not
   specified on command line.

#. Invokes “``scr_env –down``” to ask resource manager whether any nodes
   are known to be down.

#. Invokes ``ping`` to each node thought to be up.

#. Uses ``scr_param.pm`` to read ``SCR_EXCLUDE_NODES``, user may
   explicitly exclude nodes this way.

#. Adds any nodes explicitly listed on command line as being down.

#. Invokes ``scr_list_dir`` to get list of base directories for control
   directory.

#. Uses ``scr_param.pm`` to read ``CNTLDIR`` hash from config file to
   get expected capacity corresponding to each base directory.

#. Invokes ``scr_list_dir`` to get list of base directories for cache
   directory.

#. Uses ``scr_param.pm`` to read ``CACHEDIR`` hash from config file to
   get expected capacity corresponding to each base directory.

#. Invokes ``pdsh`` to run ``scr_check_node`` on each node that hasn’t
   yet failed a test.

#. Optionally print list of down nodes to stdout.

#. Optionally log each down node with reason via ``scr_log_event`` if
   logging is enabled.

#. Exit with appropriate code to indicate whether any nodes are down.

``scripts/common/scr_check_node.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Runs on compute node to execute a series of checks to verify that node
is still functioning.

#. Reads list of control directories and sizes from –cntl option.

#. Reads list of cache directories and sizes from –cache option.

#. Invokes ``ls -lt`` to check basic access for each directory.

#. If size is specified, invoke ``df`` to verify that each directory has
   sufficient space.

#. Invokes ``touch`` and ``rm -rf`` to create and delete a test file in
   each directory.

``scripts/common/scr_prefix.in``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Prints SCR prefix directory.

#. Reads ``$SCR_PREFIX`` if set.

#. Invokes ``pwd`` to get current working directory otherwise.
