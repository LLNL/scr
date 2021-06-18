#! /usr/env python

# scr_env_lsf.py
# SCR_Env_LSF is a subclass of SCR_Env_Base

import os
import scr_const, scr_hostlist
from scr_env_base import SCR_Env_Base
from scr_common import runproc

class SCR_Env_LSF(SCR_Env_Base):
  # init initializes vars from the environment
  def __init__(self):
    super(SCR_Env_LSF, self).__init__(env='LSF')

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

  # set down node list, requires node list to already be set
  def set_downnodes(self):
    # TODO: any way to get list of down nodes in LSF?
    self.conf['down'] = ''

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'],'--dir',self.conf['prefix']]
    out, returncode = runproc(argv=argv,getstdout=True)
    if returncode==0:
      return int(out)
    return 0 # print(err)

