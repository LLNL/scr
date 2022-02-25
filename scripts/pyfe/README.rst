========================================================
Python equivalents of perl/bash scripts in scr/scripts/*
========================================================

| *(Initial / tentative stage)*  
|  
| After doing the scr make install the scrjob scripts will be in scr/install/bin/pyfe  
|  
| scrjob can be installed using pip for the following usage:  
| Make a virtual environment somewhere and use pip install in scr/install/bin/pyfe  
| ``$ cd ~/scr/install/bin/pyfe``
| ``$ python3 -m venv venv``  
| ``$ source venv/bin/activate``  
| ``$ pip3 install -e .``  
| ``$ python -m scrjob.scr_srun [args]``  
|  
| scripts can be run without installing the package:  
| python3 ~/scr/install/bin/pyfe/scrjob/scr_srun.py [args]  
|  
| or ~/scr/install/bin/pyfe/scrjob can be added to the PATH and scripts ran directly:  
| ``$ scr_srun.py [args]``  
|  
| For testing, the directory ~/scr/install/bin/pyfe/tests/ will be created  
| Ensure these variables at the top of runtest.sh are appropriate values:  
| *launcher, numnodes, MPICC*  
| *(There is a sleep in scr_run.py (~line 308) which can be reduced for testing)*  
| From an allocation, run the test script:  
| ``$ cd ~/scr/install/bin/pyfe``  
| ``$ ./runtest.sh``  
| To add additional test scripts, place a file whose name matches: test*.py  
| in ~/scr/install/bin/pyfe/tests/  
|  
| The scrjob scripts will try to use the ClusterShell module  
| *this can be disabled by setting USE_CLUSTERSHELL='0' in scrjob/scr_const.py*  
| ClusterShell will not be used if it is not found or it is disabled  
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
