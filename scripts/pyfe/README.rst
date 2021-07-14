========================================================
Python equivalents of perl/bash scripts in scr/scripts/*
========================================================

| *(Initial / tentative stage)*  
|  
| After doing the scr make install the pyfe scripts will be in scr/install/bin/pyfe  
|  
| pyfe can be installed using pip for the following usage:  
| Make a virtual environment somewhere and use pip install in scr/install/bin/pyfe  
| ``$ cd ~/scr/install/bin/pyfe``
| ``$ python3 -m venv venv``  
| ``$ source venv/bin/activate``  
| ``$ pip3 install -e .``  
| ``$ python -m pyfe.scr_srun [args]``  
|  
| scripts can be ran without installing the package:  
| python3 ~/scr/install/bin/pyfe/pyfe/scr_srun.py [args]  
|  
| or ~/scr/install/bin/pyfe/pyfe can be added to the PATH and scripts ran directly:  
| ``$ scr_srun.py [args]``  
|  
| For testing: ~/scr/scripts/pyfe/test.sh will be copied to ~/scr/install/bin/pyfe
| In an allocation for 4 nodes run ./test.sh while in ~/scr/install/bin/pyfe  
| *Specify the launcher to use near the top of the script*  
| There is a sleep in scr_run.py (~line 308) which can be reduced for testing  
| ``$ salloc -N 4``
| ``$ cd ~/scr/install/bin/pyfe``
| ``$ ./test.sh``
|  
| The scripts will try to use the ClusterShell module  
| *this can be disabled by setting USE_CLUSTERSHELL='0' in pyfe/scr_const.py*  
| if clustershell is not found or it is disabled the scripts will use pdsh  
|  
| **To use clustershell instead of pdsh**  
| ``$ pip install ClusterShell``  
|  
| **Some configuration is available, described in:**  
| *clustershell.readthedocs.io/en/latest/config.html*  
| *or* ``$ man clush.conf``  
| **Node groups can be bound together by differing lists**  
|  
| *Library defaults may need to be overridden for identifying nodes*  
| *(bottom of the config.html link)*  
|  
| **Clustershell has 'nodeset' which does more ops than scr_hostlist**  
| *this provides a NodeSet class: ClusterShell.NodeSet.NodeSet*  
| **Using package: clustershell.readthedocs.io/en/latest/guide/taskmgnt.html**  
|  
