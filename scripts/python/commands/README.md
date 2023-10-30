# SCR user commands
Along with other binaries, these scripts are copied to the ``/bin`` directory of an SCR installation.
The scripts are installed with their executable bit set, and the ``.py`` suffix is dropped.
These are user commands that are typically invoked interactively or from a batch job script.
Detailed usage for the scripts in this directory is provided in the SCR user documentation.

## SCR commands

- ``scr_prerun``          - Execute before the first SCR job in an allocation
- ``scr_postrun``         - Execute after the final SCR job in an allocation; scavenges any cached datasets
- ``scr_list_down_nodes`` - Reports list of currently failed nodes in an allocation, if any
- ``scr_should_exit``     - Indicates whether one should stop launching SCR runs within an allocation; checks for active halt condition, insufficient nodes, or in sufficient time
- ``scr_halt``            - View/edit/remove conditions in the halt file  

## scr\_run

The ``scr_run`` script provides a high-level wrapper around the above scipts.
It can automatically relaunch a job and avoid down nodes after detecting a failure,
and it scavenges any cached datasets before exiting the allocation.
  
Usage:
 
Typical:
``scr_run <launcher> <launcher args> <program> <program args>``  
 
Extended options:
``scr_run <launcher> [-rc|--run-cmd]=<run command> [-rs|--restart-cmd]=<restart command> <launcher args>``  

Using extended options the commands will be modified as follows:  

If not restarting or ``scr_have_restart`` returns ``None``:  
``<launcher> <launcher args> <run command>``  

If restarting and the most recent checkpoint name is identified,  
instances of `SCR_CKPT_NAME` will be replaced with the checkpoint name in the restart command:  
``<launcher> <launcher args> <restart command>``  
