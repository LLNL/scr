========================================================
Scripts for SCR+pyfe  
========================================================
  
| While some scripts may be ran independently (outside of scr_run.py),  
| The general workflow will simply use an scr_*run.py or the scr_flux.py script  
|  
  
========================================================
scr_run.py  
========================================================
  
| Launching a jobstep using scr with the Python front end requires Python3  
| **Usage:**  
|  
| *typical*  
| ``scr_run.py <launcher> <launcher args> <program> <program args>``  
|  
| *extended options*  
| ``scr_run.py <launcher> [-rc|--run-cmd]=<run command> [-rs|--restart-cmd]=<restart command> <launcher args>``  
| Using extended options the commands will be modified as follows:  
|   If not restarting or scr_have_restart returns None:  
|     ``<launcher> <launcher args> <run command>``  
|   If restarting and the most recent checkpoint name is identified,  
|   instances of `SCR_CKPT_NAME` will be replaced with the checkpoint name in the restart command:  
|    ``<launcher> <launcher args> <restart command>``  
|  
|  **The other scr_*run.py and scr_flux.py scripts use the same semantics as scr_run.py**  
|  
| The other scripts provide a shorthand to launch scr_run.py, for example these are equivalent:  
| ``scr_run.py srun -N 1 test_api`` <==> ``scr_srun.py -N 1 test_api``  
|  
  
========================================================
Extending functionality  
========================================================
  
| **See the README in the joblauncher and resmgr subdirectories for additional guidance**
|  
  
========================================================
Scripts which may be ran outside of scr_run.py  
========================================================
  
| **While these scripts can be ran independently, to do so is generally for testing purposes**  
|  
| *scr_check_node.py*   - Check whether the control/cache directory is available, optionally checking capacity  
| *scr_common.py*       - Provides a testing interface for methods implemented within  
| *scr_const.py*        - Displays defined constant values  
| *scr_env.py*          - Print obtainable values from an allocation's environment  
| *scr_halt.py*         - View/edit/remove conditions in the halt file  
| *scr_inspect.py*      - Print a space separated list of cached datasets to try to flush/rebuild (*deprecated?*)  
| *scr_kill_jobstep.py* - Given the specified launcher and jobstepid, call Joblauncher.scr_kill_jobstep(jobstepid)  
| *scr_list_dir.py*     - Prints the value returned by list_dir.py, a space separated list of (*control/cache*) directories  
| *scr_param.py*        - Instantiates an SCR_Param object and prints its attributes  
| *scr_postrun.py*      - External driver for the postrun method  
| *scr_prerun.py*       - Executes the prerun checks called early in scr_run.py  
| *scr_scavenge.py*     - Attempt to perform a scavenge operation  
| *scr_test_runtime.py* - A subset of scr_prerun.py, performs tests determined by the ResourceManager  
| *resmgr/auto.py*      - Prints the type of ResourceManager returned by AutoResourceManager  
|  
| **Utility scripts**  
| *parsetime.py*        - Print the value returned given a time string  
| *scr_glob_hosts.py*   - Print result of node set operations with use of scr_hostlist.py  
| *scr_hostlist.py*     - Directly reference hostlist methods and view output  
|  
| **Other**  
| *scr_poststage.py*    - Intended to be called internally **(?)**, not actually used anywhere  
|  
