#! /usr/env python

import os, sys, subprocess
import scr_const

# SCR_Env class holds the configuration
#   we could pass the configuration to use (SLURM/LSF/ ...)

# def set_prefix(self,prefix):
#   the prefix should be explicitly set (?)

# def set_downnodes(self):
#   set down node list, requires node list to already be set

# def get_runnode_count(self):
#   returns the number of nodes used in the last run
#     could set a member value instead of returning a number


class SCR_Env:
  # init initializes vars from the environment 
  def __init__(self,env=None):
    if env is None:
      env = scr_const.SCR_RESOURCE_MANAGER
    self.conf = {'env':env}
    # replaces: my $scr_nodes_file = "@X_BINDIR@/scr_nodes_file"
    self.conf['nodes_file'] = scr_const.X_BINDIR+'/scr_nodes_file'
    val = os.environ.get('USER')
    if val is not None:
      self.conf['user'] = val
    else:
      self.conf['user'] = None
    val = self.getjobid()
    if val is not None:
      self.conf['jobid'] = val
    else:
      self.conf['jobid'] = None
    val = self.getnodelist()
    if val is not None:
      self.conf['nodes'] = val
    else:
      self.conf['nodes'] = None

  # get job id, setting environment flag here
  def getjobid(self):
    if self.conf['env'] == 'SLURM':
      val = os.environ.get('SLURM_JOBID')
      return val
    val = os.environ.get('LSB_JOBID')
    return val

  # get node list
  def getnodelist(self):
    if self.conf['env'] == 'SLURM':
      val = os.environ.get('SLURM_NODELIST')
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
      return val
    return None

  # set the prefix
  def set_prefix(self,prefix):
    self.conf['prefix'] = prefix

  # set down node list, requires node list to already be set
  def set_downnodes(self):
    # TODO: any way to get list of down nodes in LSF?
    if self.conf['env'] == 'LSF':
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
      return 0 # print(err)
    return int(out)

if __name__ == '__main__':
  scr_env = SCR_Env('SLURM')
  scr_env.set_downnodes()
  for key in scr_env.conf:
    print('scr_env.conf['+key+'] = \''+scr_env.conf[key]+'\'')

