.. _hash:

Hash
====

Overview
--------

A frequently used data structure is the ``scr_hash`` object. This data
structure contains an unordered list of elements, where each element
contains a key (a string) and a value (another hash). Each element in a
hash has a unique key. Using the key, one can get, set, and unset
elements in a hash. There are functions to iterate through the elements
of a hash. There are also functions to pack and unpack a hash into a
memory buffer, which enables one to transfer a hash through the network
or store the hash to a file.

Throughout the documentation and comments in the source code, a hash is
often displayed as a tree structure. The key belonging to a hash element
is shown as a parent node, and the elements in the hash belonging to
that element are displayed as children of that node. For example,
consider the following tree:

::

   +- RANK
      +- 0
      |  +- FILES
      |  |  +- 2
      |  +- FILE
      |     +- foo_0.txt
      |     |  +- SIZE
      |     |  |  +- 1024
      |     |  +- COMPLETE
      |     |     +- 1
      |     +- bar_0.txt
      |        +- SIZE
      |        |  +- 2048
      |        +- COMPLETE
      |           +- 1
      +- 1
         +- FILES
         |  +- 1
         +- FILE
            +- foo_1.txt
               +- SIZE
               |  +- 3072
               +- COMPLETE
                  +- 1

The above example represents a hash that contains a single element with
key ``RANK``. The hash associated with the ``RANK`` element contains two
elements with keys ``0`` and ``1``. The hash associated with the ``0``
element contains two elements with keys ``FILES`` and ``FILE``. The
``FILES`` element, in turn, contains a hash with a single element with a
key ``2``, which finally contains a hash having no elements.

Often when displaying these trees, the guidelines are not shown and only
the indentation is used, like so:

::

    RANK
      0
        FILES
          2
        FILE
          foo_0.txt
            SIZE
              1024
            COMPLETE
              1
          bar_0.txt
              SIZE
                2048
              COMPLETE
                1
      1
        FILES
          1
        FILE
          foo_1.txt
            SIZE
              3072
            COMPLETE
              1

Common functions
----------------

This section lists the most common functions used when dealing with
hashes. For a full listing, refer to comments in ``scr_hash.h``. The
implementation can be found in ``scr_hash.c``.

Hash basics
~~~~~~~~~~~

First, before using a hash, one must allocate a hash object.

::

     scr_hash* hash = scr_hash_new();

And one must free the hash when done with it.

::

     scr_hash_delete(&hash);

Given a hash object, you may insert an element, specifying a key and
another hash as a value.

::

     scr_hash_set(hash, key, value_hash);

If an element already exists for the specified key, this function
deletes the value currently associated with the key and assigns the
specified hash as the new value. Thus it is not necessary to unset a key
before setting it – setting a key simply overwrites the existing value.

You may also perform a lookup by specifying a key and the hash object to
be searched.

::

     scr_hash* value_hash = scr_hash_get(hash, key);

If the hash has a key by that name, it returns a pointer to the hash
associated with the key. If the hash does not have an element with the
specified key, it returns NULL.

You can unset a key.

::

     scr_hash_unset(hash, key);

If a hash value is associated with the specified key, it is freed, and
then the element is deleted from the hash. It is OK to unset a key even
if it does not exist in the hash.

To clear a hash (unsets all elements).

::

     scr_hash_unset_all(hash);

To determine the number of keys in a hash.

::

     int num_elements = scr_hash_size(hash);

To simplify coding, most hash functions accept NULL as a valid input
hash parameter. It is interpreted as an empty hash. For example,

======================================== =============================
``scr_hash_delete(NULL);``               does nothing
``scr_hash_set(NULL, key, value_hash);`` does nothing and returns NULL
``scr_hash_get(NULL, key);``             returns NULL
``scr_hash_unset(NULL, key);``           does nothing
``scr_hash_unset_all(NULL);``            does nothing
``scr_hash_size(NULL);``                 returns 0
======================================== =============================

Accessing and iterating over hash elements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

At times, one needs to work with individual hash elements. To get a
pointer to the element associated with a key (instead of a pointer to
the hash belonging to that element).

::

     scr_hash_elem* elem = scr_hash_elem_get(hash, key);

To get the key associated with an element.

::

     char* key = scr_hash_elem_key(elem);

To get the hash associated with an element.

::

     scr_hash* hash = scr_hash_elem_hash(elem);

It’s possible to iterate through the elements of a hash. First, you need
to get a pointer to the first element.

::

     scr_hash_elem* elem = scr_hash_elem_first(hash);

This function returns NULL if the hash has no elements. Then, to advance
from one element to the next.

::

     scr_hash_elem* next_elem = scr_hash_elem_next(elem);

This function returns NULL when the current element is the last element.
Below is some example code that iterates through the elements of hash
and prints the key for each element:

::

     scr_hash_elem* elem;
     for (elem = scr_hash_elem_first(hash);
          elem != NULL;
          elem = scr_hash_elem_next(elem))
     {
       char* key = scr_hash_elem_key(elem);
       printf("%s\n", key);
     }

Key/value convenience functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Often, it’s useful to store a hash using two keys which act like a
key/value pair. For example, a hash may contain an element with key
``RANK``, whose hash contains a set of elements with keys corresponding
to rank ids, where each rank id ``0``, ``1``, ``2``, etc. has a hash,
like so:

::

     RANK
       0
         <hash for rank 0>
       1
         <hash for rank 1>
       2
         <hash for rank 2>

This case comes up so frequently that there are special key/value (_kv)
functions to make this operation easier. For example, to access the hash
for rank 0 in the above example, one may call

::

     scr_hash* rank_0_hash = scr_hash_get_kv(hash, "RANK", "0");

This searches for the ``RANK`` element in the specified hash. If found,
it then searches for the ``0`` element in the hash of the ``RANK``
element. If found, it returns the hash associated with the ``0``
element. If hash is NULL, or if hash has no ``RANK`` element, or if the
``RANK`` hash has no ``0`` element, this function returns NULL.

The following function behaves similarly to ``scr_hash_get_kv`` – it
returns the hash for rank 0 if it exists. It differs in that it creates
and inserts hashes and elements as needed such that an empty hash is
created for rank 0 if it does not already exist.

::

     scr_hash* rank_0_hash = scr_hash_set_kv(hash, "RANK", "0");

This function creates a ``RANK`` element if it does not exist in the
specified hash, and it creates a ``0`` element in the ``RANK`` hash if
it does not exist. It returns the hash associated with the ``0``
element, which will be an empty hash if the ``0`` element was created by
the call. This feature lets one string together multiple calls without
requiring lots of conditional code to check whether certain elements
already exist. For example, the following code is valid whether or not
``hash`` has a ``RANK`` element.

::

     scr_hash* rank_hash = scr_hash_set_kv(hash,      "RANK", "0");
     scr_hash* ckpt_hash = scr_hash_set_kv(rank_hash, "CKPT", "10");
     scr_hash* file_hash = scr_hash_set_kv(ckpt_hash, "FILE", "3");

Often, as in the case above, the *value* key is an integer. In order to
avoid requiring the caller to convert integers to strings, there are
functions to handle the value argument as an ``int`` type, e.g, the
above segment could be written as

::

     scr_hash* rank_hash = scr_hash_set_kv_int(hash,      "RANK",  0);
     scr_hash* ckpt_hash = scr_hash_set_kv_int(rank_hash, "CKPT", 10);
     scr_hash* file_hash = scr_hash_set_kv_int(ckpt_hash, "FILE",  3);

It’s also possible to unset key/value pairs.

::

     scr_hash_unset_kv(hash, "RANK", "0");

This call removes the ``0`` element from the ``RANK`` hash if one
exists. If this action causes the ``RANK`` hash to be empty, it also
removes the ``RANK`` element from the specified input hash.

In some cases, one wants to associate a single value with a given key.
When attempting to change the value in such cases, it is necessary to
first unset a key before setting the new value. Simply setting a new
value will insert another element under the key. For instance, consider
that one starts with the following hash

::

     TIMESTEP
       20

If the goal is to modify this hash such that it changes to

::

     TIMESTEP
       21

then one should do the following

::

     scr_hash_unset(hash, "TIMESTEP");
     scr_hash_set_kv_int(hash, "TIMESTEP", 21);

Simply executing the set operation without first executing the unset
operation results in the following

::

     TIMESTEP
       20
       21

Because it is common to have fields in a hash that should only hold one
value, there are several utility functions to set and get such fields
defined in ``scr_hash_util.h`` and implemented in ``scr_hash_util.c``.
For instance, here are a few functions to set single-value fields:

::

     int scr_hash_util_set_bytecount(scr_hash* hash, const char* key, unsigned long count);
     int scr_hash_util_set_crc32(scr_hash* hash, const char* key, uLong crc);
     int scr_hash_util_set_int64(scr_hash* hash, const char* key, int64_t value);

These utility routines unset any existing value before setting the new
value. They also convert the input value into an appropriate string
representation. Similarly, there are corresponding get routines, such
as:

::

     int scr_hash_util_get_bytecount(const scr_hash* hash, const char* key, unsigned long* count);
     int scr_hash_util_get_crc32(const scr_hash* hash, const char* key, uLong* crc);
     int scr_hash_util_get_int64(const scr_hash* hash, const char* key, int64_T* value);

If a value is set for the specified key, and if the value can be
interpreted as the appropriate type for the output parameter, the get
routine returns ``SCR_SUCCESS`` and copies the value to the output
parameter. Otherwise, the routine does not return ``SCR_SUCCESS`` and
does not modify the output parameter.

For example, to set and get the timestep value from the example above,
one could do the following:

::

     scr_hash_util_set_int64(hash, "TIMESTEP", 21);

     int64_t current_timestep = -1;
     if (scr_hash_util_get_int64(hash, "TIMESTEP", &current_timestep) == SCR_SUCCESS) {
       /* TIMESTEP was set, and it's value is now in current_timestep */
     } else {
       /* TIMESTEP was not set, and current_timestep is still -1 */
     }

The difference between these utility functions and the key/value
(``_kv``) functions is that the key/value functions are used to set and
get a hash that is referenced by a key/value pair whereas the utility
functions set and get a scalar value that has no associated hash.

Specifying multiple keys with format functions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

One can set many keys in a single call using a printf-like statement.
This call converts variables like floats, doubles, and longs into
strings. It enables one to set multiple levels of keys in a single call,
and it enables one to specify the hash value to associate with the last
element.

::

     scr_hash_setf(hash, value_hash, "format", variables ...);

For example, if one had a hash like the following

::

     RANK
       0
         CKPT
           10
             <current_hash>

One could overwrite the hash associated with the ``10`` element in a
single call like so.

::

     scr_hash_setf(hash, new_hash, "%s %d %s %d", "RANK", 0, "CKPT", 10);

Different keys are separated by single spaces in the format string. Only
a subset of the printf format strings are supported.

There is also a corresponding getf version.

::

     scr_hash* hash = scr_hash_getf(hash, "%s %d %s %d", "RANK", 0, "CKPT", 10);

Sorting hash keys
~~~~~~~~~~~~~~~~~

Generally, the keys in a hash are not ordered. However, one may order
the keys with the following sort routines.

::

     scr_hash_sort(hash, direction);
     scr_hash_sort_int(hash, direction);

The first routine sorts keys by string, and the second sorts keys as
integer values. The direction variable may be either
``SCR_HASH_SORT_ASCENDING`` or ``SCR_HASH_SORT_DESCENDING``. The keys
remain in sorted order until new keys are added. The order is not kept
between packing and unpacking hashes.

Listing hash keys
~~~~~~~~~~~~~~~~~

One may get a sorted list of all keys in a hash.

::

     int num_keys;
     int* keys;
     scr_hash_list_int(hash, &num_keys, &keys);
     ...
     if (keys != NULL)
       free(keys);

This routine returns the number of keys in the hash, and if there is one
or more keys, it allocates memory and returns the sorted list of keys.
The caller is responsible for freeing this memory. Currently, one may
only get a list of keys that can be represented as integers. There is no
such list routine for arbitrary key strings.

Packing and unpacking hashes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A hash can be serialized into a memory buffer for network transfer or
storage in a file. To determine the size of a buffer needed to pack a
hash.

::

     int num_bytes = scr_hash_pack_size(hash);

To pack a hash into a buffer.

::

     scr_hash_pack(buf, hash);

To unpack a hash from a buffer into a given hash object.

::

     scr_hash* hash = scr_hash_new();
     scr_hash_unpack(buf, hash);

One must pass an empty hash to the unpack function.

Hash files
~~~~~~~~~~

Hashes may be serialized to a file and restored from a file. To write a
hash to a file.

::

     scr_hash_file_write(filename, hash);

This call creates the file if it does not exist, and it overwrites any
existing file.

To read a hash from a file (merges hash from file into given hash
object).

::

     scr_hash_file_read(filename, hash);

Many hash files are written and read by more than one process. In this
case, locks can be used to ensure that only one process has access to
the file at a time. A process blocks while waiting on the lock. The
following call blocks the calling process until it obtains a lock on the
file. Then it opens, reads, closes, and unlocks the file. This results
in an atomic read among processes using the file lock.

::

     scr_hash_read_with_lock(filename, hash)

To update a locked file, it is often necessary to execute a
read-modify-write operation. For this there are two functions. One
function locks, opens, and reads a file.

::

     scr_hash_lock_open_read(filename, &fd, hash)

The opened file descriptor is returned, and the contents of the file are
read (merged) in to the specified hash object. The second function
writes, closes, and unlocks the file.

::

     scr_hash_write_close_unlock(filename, &fd, hash)

One must pass the filename, the opened file descriptor, and the hash to
be written to the file.

Sending and receiving hashes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are several functions to exchange hashes between MPI processes.
While most hash functions are implemented in ``scr_hash.c``, the
functions dependent on MPI are implemented in ``scr_hash_mpi.c``. This
is done so that serial programs can use hashes without having to link to
MPI.

To send a hash to another MPI process.

::

     scr_hash_send(hash, rank, comm)

This call executes a blocking send to transfer a copy of the specified
hash to the specified destination rank in the given MPI communicator.
Similarly, to receive a copy of a hash.

::

     scr_hash_recv(hash, rank, comm)

This call blocks until it receives a hash from the specified rank, and
then it unpacks the received hash into ``hash`` and returns.

There is also a function to simultaneously send and receive hashes,
which is useful to avoid worrying about ordering issues in cases where a
process must both send and receive a hash.

::

     scr_hash_sendrecv(hash_send, rank_send, hash_recv, rank_recv, comm)

The caller provides the hash to be sent and the rank it should be sent
to, along with a hash to unpack the received into and the rank it should
receive from, as well as, the communicator to be used.

A process may broadcast a hash to all ranks in a communicator.

::

     scr_hash_bcast(hash, root, comm)

As with MPI, all processes must specify the same root and communicator.
The root process specifies the hash to be broadcast, and each non-root
process provides a hash into which the broadcasted hash is unpacked.

Finally, there is a call used to issue a (sparse) global exchange of
hashes, which is similar to an ``MPI_Alltoallv`` call.

::

     scr_hash_exchange(hash_send, hash_recv, comm)

This is a collective call which enables any process in ``comm`` to send
a hash to any other process in ``comm`` (including itself). Furthermore,
the destination processes do not need to know from which processes they
will receive data in advance. As input, a process should provide an
empty hash for ``hash_recv``, and it must structure ``hash_send`` in the
following manner.

::

     <rank_X>
       <hash_to_send_to_rank_X>
    <rank_Y>
      <hash_to_send_to_rank_Y>

Upon return from the function, ``hash_recv`` will be filled in according
to the following format.

::

    <rank_A>
      <hash_received_from_rank_A>
    <rank_B>
      <hash_received_from_rank_B>

For example, if ``hash_send`` was the following on rank 0 before the
call:

::

     hash_send on rank 0:
     1
       FILES
         1
       FILE
         foo.txt
     2
       FILES
         1
       FILE
         bar.txt

Then after returning from the call, ``hash_recv`` would contain the
following on ranks 1 and 2:

::

     hash_recv on rank 1:
     0
       FILES
         1
       FILE
         foo.txt
     <... data from other ranks ...>

     hash_recv on rank 2:
     0
       FILES
         1
       FILE
         bar.txt
     <... data from other ranks ...>

The algorithm used to implement this function assumes the communication
is sparse, meaning that each process only sends to or receives from a
small number of other processes. It may also be used for gather or
scatter operations.

Debugging
---------

Newer versions of TotalView enable one to dive on hash variables and
inspect them in a variable window using a tree view. For example, when
diving on a hash object corresponding to the example hash in the
overview section, one would see an expanded tree in the variable view
window like so:

::

     +- RANK
        +- 0
        |  +- FILES = 2
        |  +- FILE
        |     +- foo_0.txt
        |     |  +- SIZE = 1024
        |     |  +- COMPLETE = 1
        |     +- bar_0.txt
        |        +- SIZE = 2048
        |        +- COMPLETE = 1
        +- 1
           +- FILES = 1
           +- FILE
              +- foo_1.txt
                 +- SIZE = 3072
                 +- COMPLETE = 1

When a hash of an element contains a single element whose own hash is
empty, this display condenses the line to display that entry as a key =
value pair.

If TotalView is not available, one may resort to printing a hash to
``stdout`` using the following function. The number of spaces to indent
each level is specified in the second parameter.

::

     scr_hash_print(hash, indent);

To view the contents of a hash file, there is a utility called
``scr_print_hash_file`` which reads a file and prints the contents to
the screen.

::

     scr_print_hash_file  myhashfile.scr

Binary format
-------------

This section documents the binary format used when serializing a hash.

.. _hash_packed:

Packed hash
~~~~~~~~~~~

A hash can be serialized into a memory buffer for network transfer or
storage in a file. When serialized, all integers are stored in network
byte order (big-endian format). Such a “packed” hash consists of the
following format:

| Format of a PACKED HASH:

========== ============ ============================================
Field Name Datatype     Description
========== ============ ============================================
Count      ``uint32_t`` Number of elements in hash
\                       A count of 0 means the hash is empty.
Elements   PACKED       Sequence of packed elements of length Count.
\          ELEMENT     
========== ============ ============================================

| Format of a PACKED ELEMENT:

========== ============================ ============================
Field Name Datatype                     Description
========== ============================ ============================
Key        NULL-terminated ASCII string Key associated with element
Hash       PACKED                       Hash associated with element
\          HASH                        
========== ============================ ============================

File format
~~~~~~~~~~~

A hash can be serialized and stored as a binary file. This section
documents the file format for an ``scr_hash`` object. All integers are
stored in network byte order (big-endian format). A hash file consists
of the following sequence of bytes:

============ ============ =======================================================================================
Field Name   Datatype     Description
============ ============ =======================================================================================
Magic Number ``uint32_t`` Unique integer to help distinguish an SCR file from other types of files
\                         0x951fc3f5 (host byte order)
File Type    ``uint16_t`` Integer field describing what type of SCR file this file is
\                         1 :math:`\rightarrow` file is an ``scr_hash`` file
File Version ``uint16_t`` Integer field that together with File Type defines the file format
\                         1 :math:`\rightarrow` ``scr_hash`` file is stored in version 1 format
File Size    ``uint64_t`` Size of this file in bytes, from first byte of the header to the last byte in the file.
Flags        ``uint32_t`` Bit flags for file.
Data         PACKED       Packed hash data (see Section :ref:`1.4.1 <hash_packed>`).
\            HASH        
CRC32\*      ``uint32_t`` CRC32 of file, accounts for first byte of header to last byte of Data.
\                         \*Only exists if ``SCR_FILE_FLAGS_CRC32`` bit is set in Flags.
============ ============ =======================================================================================
