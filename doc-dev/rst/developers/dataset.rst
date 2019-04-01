.. _datasets:

Datasets
========

Overview
--------

The ``scr_dataset`` data structure associates various attributes with
each dataset written by the application. It tracks information such as
the dataset id, the creation time, the total number of bytes.

The ``scr_hash`` is utilized in the ``scr_dataset`` API and its
implementation. Essentially, ``scr_dataset`` objects are specialized
``scr_hash`` objects that have certain well-defined keys (*fields*) and
associated functions to access those fields.

Example dataset hash
--------------------

Internally, dataset objects are implemented as ``scr_hash`` objects.
Here is an example hash for a dataset object.

::

     ID
       23
     USER
       user1
     JOBNAME
       simulation123
     NAME
       dataset.23
     SIZE
       524294000
     FILES
       1024
     CREATED
       1312850690668536
     CKPT
       6
     COMPLETE
       1

The ``ID`` field records the dataset id of the dataset as assigned by
the ``scr_dataset_id`` variable at the time the dataset is created. The
``USER`` field records the username associated with the job within which
the dataset was created, and the value of ``$SCR_JOB_NAME``, if set, is
recorded in the ``JOBNAME`` field. The ``NAME`` field records the name
of the dataset. This is currently defined to be “``dataset.<id>``” where
``<id>`` is the dataset id. The total number of bytes in the dataset is
recorded in the ``SIZE`` field, and the total number of files is
recorded in ``FILES``. The ``CREATED`` field records the time at which
the dataset was created, in terms of microseconds since the Linux epoch.
If the dataset is a checkpoint, the checkpoint id is recorded in the
``CKPT`` field. The ``COMPLETE`` field records whether the dataset is
valid. It is set to 1 if the dataset is thought to be valid, and 0
otherwise.

These are the most common fields used in dataset objects. Not all fields
are required, and additional fields may be used that are not shown here.

Common functions
----------------

This section describes some of the most common dataset functions. For a
detailed list of all functions, see ``scr_dataset.h``. The
implementation can be found in ``scr_dataset.c``.

Allocating and freeing dataset objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a new dataset object.

::

     scr_dataset* dataset = scr_dataset_new()

Free a dataset object.

::

     scr_dataset_delete(&dataset);

Setting, getting, and checking field values
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are functions to set each field individually.

::

     int scr_dataset_set_id(scr_dataset* dataset, int id);
     int scr_dataset_set_user(scr_dataset* dataset, const char* user);
     int scr_dataset_set_jobname(scr_dataset* dataset, const char* name);
     int scr_dataset_set_name(scr_dataset* dataset, const char* name);
     int scr_dataset_set_size(scr_dataset* dataset, unsigned long size);
     int scr_dataset_set_files(scr_dataset* dataset, int files);
     int scr_dataset_set_created(scr_dataset* dataset, int64_t created);
     int scr_dataset_set_jobid(scr_dataset* dataset, const char* jobid);
     int scr_dataset_set_cluster(scr_dataset* dataset, const char* name);
     int scr_dataset_set_ckpt(scr_dataset* dataset, int id);
     int scr_dataset_set_complete(scr_dataset* dataset, int complete);

If a field was already set to a value before making this call, the new
value overwrites any existing value.

And of course there are corresponding functions to get values.

::

     int scr_dataset_get_id(const scr_dataset* dataset, int* id);
     int scr_dataset_get_user(const scr_dataset* dataset, char** name);
     int scr_dataset_get_jobname(const scr_dataset* dataset, char** name);
     int scr_dataset_get_name(const scr_dataset* dataset, char** name);
     int scr_dataset_get_size(const scr_dataset* dataset, unsigned long* size);
     int scr_dataset_get_files(const scr_dataset* dataset, int* files);
     int scr_dataset_get_created(const scr_dataset* dataset, int64_t* created);
     int scr_dataset_get_jobid(const scr_dataset* dataset, char** jobid);
     int scr_dataset_get_cluster(const scr_dataset* dataset, char** name);
     int scr_dataset_get_ckpt(const scr_dataset* dataset, int* id);
     int scr_dataset_get_complete(const scr_dataset* dataset, int* complete);

If the corresponding field is set, the get functions copy the value into
the output parameter and return ``SCR_SUCCESS``. If ``SCR_SUCCESS`` is
not returned, the output parameter is not changed.
