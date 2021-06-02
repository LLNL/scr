#! /usr/env python

import os
import sys, subprocess
from scr_scavenge import SCR_Scavenge

# SCR_Env class holds the configuration
#   this class could be passed to other methods:
#     SCR_Scavenge(scr_env)
#   then the scavenge function runs using this env

# the init takes any global constants:
#   scr_env = SCR_Env(bindir='')
# the init discovers the env from the environment variables
#   init sets -> conf [ 'user', 'env', 'jobid', 'nodes' ]
#     'env' values: 'SLURM', 'LSF', ..., 'unknown'

# def set_prefix(self,prefix):
#   the prefix should be explicitly set

# def set_downnodes(self):
#   set down node list, requires node list to already be set

# def get_runnode_count(self):
#   returns the number of nodes used in the last run
#     could set a member value instead of returning a number


class SCR_Env:
  # init initializes vars from the environment 
  # where there is system variation we use methods
  def __init__(self,bindir=''):
    # the get env methods set a flag, e.g., conf['env'] = 'SLURM'
    self.conf = {}
    self.conf['BINDIR'] = bindir
    # replaces: my $scr_nodes_file = "@X_BINDIR@/scr_nodes_file"
    self.conf['nodes_file'] = bindir+'/scr_nodes_file'
    val = os.environ.get('USER')
    if val is not None:
      self.conf['user'] = val
    else:
      sys.exit(1)
    val = self.getjobid()
    if val is not None:
      self.conf['jobid'] = val
    else:
      print('defjobid')
    val = self.getnodelist()
    if val is not None:
      self.conf['nodes'] = val
    else:
      sys.exit(1)

  # get job id, setting environment flag here
  def getjobid(self):
    val = os.environ.get('SLURM_JOBID')
    if val is not None:
      self.conf['env'] = 'SLURM'
      return val
    val = os.environ.get('LSB_JOBID')
    if val is not None:
      self.conf['env'] = 'LSF'
      return val
    self.conf['env'] = 'unknown'
    return None

  # get node list
  def getnodelist(self):
    if self.conf['env'] == 'SLURM':
      val = os.environ.get('SLURM_NODELIST')
      if val is not None:
        return val
    elif self.conf['env'] == 'LSF':
      val = os.environ.get('LSB_DJOB_HOSTFILE')
      if val is not None:
        with open(val,'r') as hostfile:
          # make a list from the set -> make a set from the list -> file.readlines().rstrip('\n')
          hosts = list(set([line.rstrip('\n') for line in hostfile.readlines()]))
          # compress host list ###
          return hosts
      val = os.environ.get('LSB_HOSTS')
      if val is not None:
        # compress hostlist ###
        return val
    return None

  # set the prefix
  def set_prefix(self,prefix):
    self.conf['prefix'] = prefix

  # set down node list, requires node list to already be set
  def set_downnodes(self):
    # TODO: any way to get list of down nodes in LSF?
    if self.conf['env'] == 'LSF':
      #sys.exit(1)
      return
    argv = ['sinfo','-ho','%N','-t','down','-n',self.conf['nodes']]
    runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    out, err = runproc.communicate()
    if runproc.returncode!=0:
      #print('0')
      print(err)
      sys.exit(1)
    self.conf['down'] = out # parse out

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'],'--dir',self.conf['prefix']]
    runproc = subprocess.Popen(args=argv, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, universal_newlines=True)
    out, err = runproc.communicate()
    if runproc.returncode!=0:
      print('0') # print(err)
      sys.exit(1)
    return out

if __name__ == '__main__':
  scr_env = SCR_Env()
  scr_env.set_downnodes()
  for key in scr_env.conf:
    print(f'scr_env.conf[{key}] = \'{scr_env.conf[key]}\'')
