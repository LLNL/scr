#! /usr/bin/env python3

# scr_joblauncher_mpirun.py
# The SCR_Joblauncher_mpirun class provides interpretation for the mpirun launcher

from pyfe import scr_hostlist
from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_mpirun(SCR_Joblauncher_Base):
  def __init__(self,launcher='mpirun'):
    super(SCR_Joblauncher_mpirun, self).__init__(launcher=launcher)

  def get_hostfile_hosts(downlist=[]):
    # get the file name and read the file
    val = os.environ.get('LSB_DJOB_HOSTFILE')
    if val is None:
      return None
    # LSB_HOSTS would be easier, but it gets truncated at some limit
    # only reliable way to build this list is to process file specified
    # by LSB_DJOB_HOSTFILE
    hosts = []
    try:
      # got a file, try to read it
      with open(val,'r') as hostfile:
        hosts = [line.strip() for line in hostfile.readlines()]
      if len(hosts)==0:
        raise ValueError('Hostfile empty')
    except:
      return None
    # build set of unique hostnames, one hostname per line
    uniquehosts = []
    for host in hosts:
      if len(host)==0:
        continue
      if host not in uniquehosts and host not in downlist:
        uniquehosts.append(host)
    return uniquehosts

  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    if len(launcher_args)==0:
      return []
    target_hosts = self.get_hostfile_hosts(downlist=scr_hostlist.expand(down_nodes))
    try:
      with open(self.conf['hostfile'],'w') as hostfile:
        print(','.join(target_hosts),file=hostfile)
      argv = [self.conf['launcher'],'--hostfile',self.conf['hostfile']]
      argv.extend(launcher_args)
      return argv
    except Exception as e:
      print('scr_mpirun: Error writing hostfile and creating launcher command')
      print('launcher file: \"'+self.conf['hostfile']+'\"')
      return []
