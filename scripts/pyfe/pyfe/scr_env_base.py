#! /usr/env python

# scr_env_base
# SCR_Env_Base is the super class of the specific environment classes
# the SCR_Env_Base constructor takes an env argument
# env can be: 'SLURM', 'LSF', 'APRUN', 'PMIX', None
# if env is None we check scr_const.SCR_RESOURCE_MANAGER
# returns appropriate environment class according to env value
# common methods shared between subclasses can be used from here
# (the __init__ is currently shared, all environments will use the super init)
# default functionality is given in the base class which subclasses may or may not override

import os, sys, subprocess
import scr_const, scr_hostlist
import scr_env_slurm, scr_env_lsf, scr_env_aprun, scr_env_pmix

# def set_prefix(self,prefix):
#   the prefix should be explicitly set (?)

# def set_downnodes(self):
#   set down node list, requires node list to already be set

# def get_runnode_count(self):
#   returns the number of nodes used in the last run
#     could set a member value instead of returning a number

class SCR_Env_Base(object):
  def __new__(cls,env=None):
    if env is None:
      env = scr_const.SCR_RESOURCE_MANAGER
    if env=='SLURM':
      return scr_env_slurm.SCR_Env_SLURM()
    elif env=='APRUN':
      return scr_env_aprun.SCR_Env_APRUN()
    elif env=='PMIX':
      return scr_env_pmix.SCR_Env_PMIX()
    elif env=='LSF':
      return scr_env_lsf.SCR_Env_LSF()
    return super(SCR_Env_Base, cls).__new__(cls)

  def __init__(self,env=None):
    self.conf = {}
    self.conf['env'] = env
    self.conf['nodes_file'] = scr_const.X_BINDIR+'/scr_nodes_file'
    self.conf['user'] = os.environ.get('USER')
    self.conf['jobid'] = self.getjobid()
    self.conf['nodes'] = self.get_job_nodes()

  def getjobid(self):
    # failed to read jobid from environment,
    # assume user is running in test mode
    return 'defjobid'

  # get node list
  def get_job_nodes(self):
    return None

  def get_downnodes(self):
    return None

  # set the prefix
  def set_prefix(self,prefix):
    self.conf['prefix'] = prefix

  # set down node list, requires node list to already be set
  def set_downnodes(self):
    self.conf['down'] = ''

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    return 0 # print(err)

