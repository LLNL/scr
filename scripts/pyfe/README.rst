========================================================
Python equivalents of perl/bash scripts in scr/scripts/*
========================================================

| *(Initial / tentative stage)*  
|  
| After doing the scr make install the pyfe scripts will be in scr/install/bin/pyfe  
|  
| pyfe can be installed using pip for the following usage:  
| Make a virtual environment somewhere and use pip install in scr/install/bin/pyfe  
| ``$ cd ~/scr/install/bin``
| ``$ python3 -m venv venv``  
| ``$ source venv/bin/activate``  
| ``$ cd pyfe/``
| ``$ pip3 install -e .``  
| ``$ python -m pyfe.scr_srun [args]``  
|  
| or scr/install/bin/pyfe can be added to the PATH and scripts ran directly  
| ``$ scr_srun.py [args]``  
|  
| For testing scr/scripts/pyfe/test.sh will be copied to scr/install/bin/pyfe
| Get an allocation for 4 nodes then run the testing script while in scr/install/bin/pyfe  
| Specify the launcher to use near the top of the script  
| There is a long sleep in scr_run.py (60 seconds) which can be reduced for testing  
| ``$ salloc -N 4``
| ``$ cd ~/scr/install/bin/pyfe``
| ``$ ./test.sh``
|  
| *clustershell not yet implemented*  
| **To use clustershell instead of pdsh**  
| ``$ pip install ClusterShell``  
| **Some configuration is available, described in:**  
| *clustershell.readthedocs.io/en/latest/config.html*  
| *or* ``$ man clush.conf``  
| **Node groups can be bound together by differing lists**  
|  
| *Library defaults may need to be overriden for identifying nodes*  
| *(bottom of the config.html link)*  
|  
| *The clustershell package also provides a bin 'clush'*  
| *(this is just a simple launcher script)*  
|  
| **Clustershell has 'nodeset' which does more ops than scr_hostlist**  
| *this provides a NodeSet class: ClusterShell.NodeSet.NodeSet*  
| **Using package: clustershell.readthedocs.io/en/latest/guide/taskmgnt.html**  
|  
| **provides clubak which works similar to dshbak for pdsh**  
| *from ClusterShell.CLI.Clubak import main*  
|  
