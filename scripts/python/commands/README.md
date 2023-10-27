# SCR user commands
Along with other binaries, these scripts are copied to the /bin directory of an SCR installation.
These are user commands that are typically invoked interactively or from a batch job script.
Detailed usage for the scripts in this directory is provided in the SCR user documentation.

- *scr_prerun.py*          - Execute before the first SCR job in an allocation
- *scr_postrun.py*         - Execute after the final SCR job in an allocation; scaveneges any cached datasets
- *scr_list_down_nodes.py* - Reports list of currently failed nodes in an allocation, if any
- *scr_should_exit.py*     - Indicates whether one should stop launching SCR runs within an allocation; checks for active halt condition, insufficient nodes, or in sufficient time
- *scr_halt.py*            - View/edit/remove conditions in the halt file  

## scr_run.py  

The scr_run.py script and its variants provide a high-level wrapper around the above scipts.
They can automatically relaunch a job and avoid down nodes after detecting a failure,
and they scavenge any cached datasets before exiting the allocation.
  
Launching a jobstep using scr with the Python front end requires Python3  
**Usage:**  
 
*typical*  
``scr_run.py <launcher> <launcher args> <program> <program args>``  
 
*extended options*  
``scr_run.py <launcher> [-rc|--run-cmd]=<run command> [-rs|--restart-cmd]=<restart command> <launcher args>``  

Using extended options the commands will be modified as follows:  

  If not restarting or scr_have_restart returns None:  
    ``<launcher> <launcher args> <run command>``  

  If restarting and the most recent checkpoint name is identified,  
  instances of `SCR_CKPT_NAME` will be replaced with the checkpoint name in the restart command:  
   ``<launcher> <launcher args> <restart command>``  
 
 **The other scr_*run.py and scr_flux.py scripts use the same semantics as scr_run.py**  
 
The other scripts provide a shorthand to launch scr_run.py, for example these are equivalent:  
``scr_run.py srun -N 1 test_api`` <==> ``scr_srun.py -N 1 test_api``  
