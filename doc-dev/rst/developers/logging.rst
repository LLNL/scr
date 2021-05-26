.. _logging:

Logging
=======

If enabled, the SCR library and the SCR scripts log different events, recording things like:

- start time and end time of each application run
- time consumed to write each checkpoint/output dataset
- time consumed to transfer each dataset from cache to the file system
- any application restarts
- any node failures

This info will let us do various things like:

- gather stats about datasets, including the number of procs used, number of files, and total byte size of each dataset, which could help inform decisions about cache storage requirements for current/future systems
- gather stats about SCR and I/O system performance and variability
- compute stats about application interrupts on a machine
- compute optimal checkpoint frequency for each application.  For this, we have a script that parses log entries from the text log file and computes the optimal checkpoint frequency using either the Young or Daly formulas.  The goal is to integrate a script like that into the scr_srun script to set an application checkpoint interval more dynamically based on what the application is experiencing on each system: ``scr_ckpt_interval.py``

There are 3 working logging mechanisms.  One can use them in any combination in a run:

- text file written to the application's SCR prefix directory.  This is most useful for end users.
- messages written to syslog.  This collects log messages from all jobs running on the system, so it is most useful to system support staff
- records are written to a MySQL database.  This could be used by either an end user or the system support staff.  It may be redundant with the other two, but it still works, so I kept it in there.  This file has the commands that creates the MySQL database: ``scr.mysql``

Settings
--------

There are settings for each logging mechanism:

- ``SCR_LOG_ENABLE`` - if 0, this disables *all* logging no matter what other settings are.  It is there as an easy way to turn off all logging.  If set to 1, then logging depends on other settings below.
- Text-based logging:

  - ``SCR_LOG_TXT_ENABLE`` - if 1, the text log file is enabled.

- Syslog-based logging:

  - ``SCR_LOG_SYSLOG_ENABLE`` - if 1, syslog messages are enabled.  There are some associated configure-time settings:
  - ``-DSCR_LOG_SYSLOG_FACILITY=[facility]`` : Facility for syslog messages (see man openlog), defaults to ``LOG_LOCAL7``
  - ``-DSCR_LOG_SYSLOG_LEVEL=[level]`` : Level for syslog messages (see man openlog), defaults to ``LOG_INFO``
  - ``-DSCR_LOG_SYSLOG_PREFIX=[str]`` : Prefix string to prepend to syslog messages, defaults to "SCR"

- MySQL-based logging:

  - ``SCR_LOG_DB_ENABLE`` - if 1, mysql logging is enabled
  - ``SCR_LOG_DB_DEBUG`` - if 1, echo sql statements to stdout to help when debugging mysql problems
  - ``SCR_LOG_DB_HOST`` - host name of mysql server
  - ``SCR_LOG_DB_NAME`` - database name on mysql server
  - ``SCR_LOG_DB_USER`` - username for accessing database
  - ``SCR_LOG_DB_PASS`` - password for accessing database
