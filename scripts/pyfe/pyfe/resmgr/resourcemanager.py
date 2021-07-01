#! /usr/bin/env python3

# scr_resourcemgr_base
# SCR_Resourcemgr_Base is the super class of the specific resource manager classes
# the SCR_Resourcemgr_Base itself is missing its environment type (unknown)
# common methods shared between subclasses can be used from here
# (all environments currently also call the super init)

import os
from pyfe import scr_const, scr_hostlist
from pyfe.scr_common import scr_prefix, runproc

class ResourceManager(object):
  def __init__(self,resmgr='unknown'):
    self.conf = {}
    self.conf['prefix'] = scr_prefix()
    self.conf['resmgr'] = resmgr
    self.conf['use_watchdog'] = False
    self.conf['jobid'] = self.getjobid()
    self.conf['nodes'] = self.get_job_nodes()

  # no arg -> usewatchdog will return True or False for whether or not watchdog is enabled
  # boolean arg -> default value is to set self.use_watchdog = argument
  # override this method for a specific resource manager to disable use of scr_watchdog
  def usewatchdog(self,use_scr_watchdog=None):
    if use_scr_watchdog is None:
      return self.conf['use_watchdog']
    self.conf['use_watchdog'] = use_scr_watchdog

  def getjobid(self):
    # failed to read jobid from environment,
    # assume user is running in test mode
    return None

  # get node list
  def get_job_nodes(self):
    return None

  def get_downnodes(self):
    return None

  def get_jobstep_id(self,user='',pid=-1):
    return -1

  def scr_kill_jobstep(self,jobid=-1):
    return 1

  def get_scr_end_time(self):
    return 0

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[],scr_env=None,free=False):
    return {}

if __name__=='__main__':
  resmgr = ResourceManager()
  print(type(resmgr))
