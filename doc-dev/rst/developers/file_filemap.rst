.. _filemap_file:

Filemap files
-------------

To efficiently support multiple processes per node, several files are
used to record the files stored in cache. Each process reads and writes
its own filemap file, named ``filemap_#.scrinfo``, where # is the rank
of the process in ``scr_comm_local``. Additionally, the master rank on
each node writes a file named ``filemap.scrinfo``, which lists the file
names for all of the filemap files. These files are all written to the
SCR control directory.

For example, if there are 4 processes on a node, then the following
files would exist in the SCR control directory.

::

     filemap.scrinfo
     filemap_0.scrinfo
     filemap_1.scrinfo
     filemap_2.scrinfo
     filemap_3.scrinfo

The contents of each ``filemap_#.scrinfo`` file would look something
like the example in
Section :ref:`Example filemap hash <filemap_example>`. The contents
of ``filemap.scrinfo`` would be the following:

::

     Filemap
         /<path_to_filemap_0>/filemap_0.scrinfo
         /<path_to_filemap_1>/filemap_1.scrinfo
         /<path_to_filemap_2>/filemap_2.scrinfo
         /<path_to_filemap_3>/filemap_3.scrinfo

With this setup, the master rank on each node writes ``filemap.scrinfo``
once during ``SCR_Init()`` and each process is then free to access its
own filemap file independently of all other processes running on the
node. The full path to each filemap file is specified to enable these
files to be located in different directories. Currently all filemap
files are written to the control directory.

During restart or during a scavenge, it is necessary for a newly started
process to build a complete filemap of all files on a node. To do this,
the process first reads ``filemap.scrinfo`` to get the names of all
filemap files, and then it reads each filemap file using code like the
following:

::

     /* read master filemap to get the names of all filemap files */
     struct scr_hash* maps = scr_hash_new();
     scr_hash_read("...filemap.scrinfo", maps);

     /* create an empty filemap and read in each filemap file */
     struct scr_hash_elem* elem;
     scr_filemap* map = scr_filemap_new();
     for (elem = scr_hash_elem_first(maps, "Filemap");
          elem != NULL
          elem = scr_hash_elem_next(elem))
     {
       char* file = scr_hash_elem_key(elem);
       scr_filemap_read(file, map)
     }
