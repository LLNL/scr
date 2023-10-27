# Wrappers for SCR command line interface
The classes in this directory provide python interfaces around SCR command line tools.

- ``scr_index.py``      - wraps ``bin/scr_index`` for accessing the index file
- ``scr_flush_file.py`` - wraps ``libexec/scr_flush_file`` for accessing the flush file
- ``scr_nodes_file.py`` - wraps ``libexec/scr_nodes_file`` for accessing the nodes file
- ``scr_halt_cntl.py``  - wraps ``libexec/scr_halt_cntl`` for accessing the halt file
- ``scr_log.py``        - wraps ``libexec/scr_log_event`` and ``libexec/scr_log_transfer`` for submitting log entries
