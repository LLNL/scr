========================================================
Python equivalents of perl/bash scripts in scr/scripts/*
========================================================

| *(Initial / tentative stage)*  
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
