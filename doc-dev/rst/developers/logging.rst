.. _logging:

Logging
=======

If enabled, the SCR library and the SCR scripts log different events, recording things like:

- start time and end time of each application run
- time consumed to write each checkpoint/output dataset
- time consumed to transfer each dataset from cache to the file system
- any application restarts
- any node failures

This info can enable one to do things like:

- Gather stats about datasets, including the number of processes used, number of files, and total byte size of each dataset, which could help inform decisions about cache storage requirements for current/future systems.
- Gather stats about SCR and I/O system performance and variability.
- Compute stats about application interrupts on a machine.
- Compute optimal checkpoint frequency for each application.  For this, there is a script that parses log entries from the text log file and computes the optimal checkpoint frequency using the Young or Daly formulas.  The goal is to integrate a script like that into the scr_srun script to set an application checkpoint interval dynamically based on what the application is experiencing on each system: :code:`scr_ckpt_interval.py`.

There are three working logging mechanisms.  One can use them in any combination in a run:

- Text file written to the application's SCR prefix directory.  This is most useful for end users.
- Messages written to syslog.  This collects log messages from all jobs running on the system, so it is most useful to system support staff.
- Records written to a MySQL database.  This could be used by either an end user or the system support staff.  To create the MySQL database, see :code:`scr.mysql`.

Settings
--------

There are settings for each logging mechanism:

- :code:`SCR_LOG_ENABLE` - If 0, this disables *all* logging no matter what other settings are.  It is there as an easy way to turn off all logging.  If set to 1, then logging depends on other settings below.
- Text-based logging:

  - :code:`SCR_LOG_TXT_ENABLE` - If 1, the text log file is enabled.

- Syslog-based logging:

  - :code:`SCR_LOG_SYSLOG_ENABLE` - If 1, syslog messages are enabled.  There are some associated configure-time settings.
  - :code:`-DSCR_LOG_SYSLOG_FACILITY=[facility]` : Facility for syslog messages (see :code:`man openlog`), defaults to :code:`LOG_LOCAL7`
  - :code:`-DSCR_LOG_SYSLOG_LEVEL=[level]` : Level for syslog messages (see :code:`man openlog`), defaults to :code:`LOG_INFO`
  - :code:`-DSCR_LOG_SYSLOG_PREFIX=[str]` : Prefix string to prepend to syslog messages, defaults to :code:`"SCR"`

- MySQL-based logging:

  - :code:`SCR_LOG_DB_ENABLE` - If 1, MySQL logging is enabled.
  - :code:`SCR_LOG_DB_DEBUG` - If 1, echo SQL statements to :code:`stdout` to help when debugging MySQL problems.
  - :code:`SCR_LOG_DB_HOST` - Hostname of MySQL server.
  - :code:`SCR_LOG_DB_NAME` - Database name on MySQL server.
  - :code:`SCR_LOG_DB_USER` - Username for accessing database.
  - :code:`SCR_LOG_DB_PASS` - Password for accessing database.
