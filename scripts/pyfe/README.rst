========================================================
Python equivalents of perl/bash scripts in scr/scripts/*
========================================================

| *(Initial / tentative stage)*  
|  
| For testing there are 2 scripts in scr/scripts/pyfe copied from testing/TESTING.sh  
| lsftest.sh and slurmtest.sh  
| Get an allocation for 4 nodes then run the script while in scr/scripts/pyfe/  
|   
| After doing the scr make install the pyfe scripts will be in scr/install/bin/pyfe  
| Make a virtual environment somewhere and use pip install in scr/install/bin/pyfe  
| ``$ cd ~/scr/install/bin``
| ``$ python3 -m venv venv``  
| ``$ source venv/bin/activate``  
| ``$ cd pyfe/``
| ``$ pip3 install -e .``  
|  
| With the package installed, we should be able to get an allocation of 4 nodes  
| and run the testing scripts  
| ``$ salloc -N 4``
| ``$ cd ~/scr/scripts/pyfe``
| ``$ ./slurmtest.sh``
|  
| **Then we can run scripts from any directory as:**  
| ``$ python -m scr_run <launcher> [args]``  
| ``$ python -m scr_srun [args]``  
| ``$ python -m scr_list_down_nodes``
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
