#! /usr/env python

# scr_env_slurm.py
# SCR_Env_SLURM is a subclass of SCR_Env_Base

import os, sys, subprocess
import scr_const, scr_hostlist
from scr_env_base import SCR_Env_Base
from scr_common import runproc

# SCR_Env class holds the configuration

class SCR_Env_SLURM(SCR_Env_Base):
  # init initializes vars from the environment
  def __init__(self):
    super(SCR_Env_SLURM, self).__init__(env='SLURM')

  # get job id, setting environment flag here
  def getjobid(self):
    val = os.environ.get('SLURM_JOBID')
    if val is not None:
      return val
    # failed to read jobid from environment,
    # assume user is running in test mode
    return 'defjobid'

  # get node list
  def get_job_nodes(self):
    return os.environ.get('SLURM_NODELIST')

  def get_downnodes(self):
    val = os.environ.get('SLURM_NODELIST')
    if val is not None:
      argv = ['sinfo','-ho','%N','-t','down','-n',val]
      down, returncode = runproc(argv=argv,getstdout=True)
      if returncode == 0:
        return down.rstrip()
    return None

  # set down node list, requires node list to already be set
  def set_downnodes(self):
    if self.conf['nodes'] is None:
      self.conf['down'] = ''
      return
    argv = ['sinfo','-ho','%N','-t','down','-n',','.join(self.conf['nodes'])]
    out, returncode = runproc(argv=argv)
    if returncode!=0:
      #print('0')
      print(out[1])
      sys.exit(1)
    self.conf['down'] = out[0] # parse out

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'],'--dir',self.conf['prefix']]
    out, returncode = runproc(argv=argv,getstdout=True)
    if returncode==0:
      return int(out)
    return 0 # print(err)

