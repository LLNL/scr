#! /usr/bin/env python3

# scr_resourcemgr_lsf.py
# SCR_Resourcemgr_LSF is a subclass of SCR_Resourcemgr_Base

import os
from pyfe import scr_const, scr_hostlist
from pyfe.resmgr.scr_resourcemgr_base import SCR_Resourcemgr_Base
from pyfe.scr_common import runproc

class SCR_Resourcemgr_LSF(SCR_Resourcemgr_Base):
  # init initializes vars from the environment
  def __init__(self):
    super(SCR_Resourcemgr_LSF, self).__init__(resmgr='LSF')

  # get job id, setting environment flag here
  def getjobid(self):
    val = os.environ.get('LSB_JOBID')
    if val is not None:
      return val
    # failed to read jobid from environment,
    # assume user is running in test mode
    return 'defjobid'

  # get node list
  def get_job_nodes(self):
    val = os.environ.get('LSB_DJOB_HOSTFILE')
    if val is not None:
      with open(val,'r') as hostfile:
        # make a list from the set -> make a set from the list -> file.readlines().rstrip('\n')
        # get a list of lines without newlines and skip the first line
        lines = [line.rstrip() for line in hostfile.readlines()][1:]
        # get a set of unique hostnames, convert list to set and back
        hosts_unique = list(set(lines))
        hostlist = scr_hostlist.compress(hosts_unique)
        return hostlist
    val = os.environ.get('LSB_HOSTS')
    # perl code called scr_hostlist.compress
    # that method takes a list though, not a string
    return val

  def get_downnodes(self):
    val = os.environ.get('LSB_HOSTS')
    if val is not None:
      # TODO : any way to get list of down nodes in LSF?
      pass
    return None

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'],'--dir',self.conf['prefix']]
    out, returncode = runproc(argv=argv,getstdout=True)
    if returncode==0:
      return int(out.rstrip())
    return 0 # print(err)

