========================================================
Python scripts for SCR  
========================================================
  
| While some scripts may be run independently (outside of scr_run.py),  
| The general workflow will simply use an scr_*run.py or the scr_flux.py script  
|  
  
========================================================
Extending functionality  
========================================================
  
| **See the README in the cli, launchers, and resmgrs directories for additional guidance**
|  
  
========================================================
Scripts which may be run outside of scr_run.py  
========================================================
  
| **While these scripts can be run independently, to do so is generally for testing purposes**  
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
| *scr_scavenge.py*     - Attempt to perform a scavenge operation  
| *scr_test_runtime.py* - A subset of scr_prerun.py, performs tests determined by the ResourceManager  
| *resmgr/auto.py*      - Prints the type of ResourceManager returned by AutoResourceManager  
|  
| **Utility scripts**  
| *parsetime.py*        - Print the value returned given a time string  
| *scr_hostlist.py*     - Directly reference hostlist methods and view output  
|  
| **Other**  
| *scr_poststage.py*    - Intended to be called internally **(?)**, not actually used anywhere  
|  
