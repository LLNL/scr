========================================================
Python equivalents of perl/bash scripts in scr/scripts/*
========================================================

| *(Initial / tentative stage)*  
|  
| An scr_const.py.in is in the SLURM and LSF folders  
| If a test is made for another resource manager, copy scr_const.py into that folder as scr_const.py.in  
| For testing, copy the scr/scripts/pyfe/CMakelists.txt to scr/scripts  
| Then running the make install will put a filled in scr_const.py in scr/install/bin/  
| Copy that file to scr/scripts/pyfe/pyfe/  
| Then there are 2 tentative testings scripts in scr/scripts/pyfe:  
| lsftest.sh and slurmtest.sh  
| These were copied from scr/testing/TESTING.sh  
| Get an allocation for 4 nodes then run the script while in scr/scripts/pyfe/  
|   
| **Using the setup.py in this directory, we can do:**  
| ``$ python3 -m venv venv``  
| ``$ source venv/bin/activate``  
| ``$ pip3 install -e .``  
|   
| **Then we can run a launcher as:**  
| ``$ python3 scr_run [args]``  
| ``$ python3 scr_srun [args]``  
| ``$ python3 scr_jsrun [args]``  
|  
|  **To use clustershell instead of pdsh**  
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
