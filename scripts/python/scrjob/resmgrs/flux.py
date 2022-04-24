#! /usr/bin/env python3

# FLUX is a subclass of ResourceManager

import os, re
import datetime
from time import time

from scrjob import scr_const
from scrjob.resmgrs import nodetests, ResourceManager

# flux imports
try:
  import flux
  from flux.hostlist import Hostlist
  from flux.resource import ResourceSet
  from flux.rpc import RPC
except:
  pass

class FLUX(ResourceManager):
  # init initializes vars from the environment
  def __init__(self):
    try:
      self.flux = flux.Flux()
    except:
      raise ImportError('Error importing flux, ensure that the flux daemon is running.')
    # the super.init() calls resmgr.get_job_nodes, we must set self.flux first
    super(FLUX, self).__init__(resmgr='FLUX')
    ### set the jobid once at init
    self.jobid = None
    self.jobid = self.getjobid()

  ####
  # the job id of the allocation is needed in postrun/list_dir
  # the job id is a component of the path.
  # We can either copy methods from existing resource managers . . .
  # or we can use the POSIX timestamp and set the value at __init__
  def getjobid(self):
    if self.jobid is not None:
      return self.jobid
    if scr_const.SCR_RESOURCE_MANAGER == 'SLURM':
      return os.environ.get('SLURM_JOBID')
    if scr_const.SCR_RESOURCE_MANAGER == 'LSF':
      return os.environ.get('LSB_JOBID')
    if scr_const.SCR_RESOURCE_MANAGER == 'APRUN':
      return os.environ.get('PBS_JOBID')
    timestamp = str(int(time()))
    return timestamp

  # get node list
  def get_job_nodes(self):
    resp = RPC(self.flux, "resource.status").get()
    rset = ResourceSet(resp["R"])
    return str(rset.nodelist)

  def get_downnodes(self):
    downnodes = {}
    resp = RPC(self.flux, "resource.status").get()
    rset = ResourceSet(resp["R"])
    offline = str(resp['offline'])
    exclude = str(resp['exclude'])
    offline = self.expand_hosts(offline)
    exclude = self.expand_hosts(offline)
    for node in offline:
      if node != '' and node not in downnodes:
        downnodes[node] = 'Reported down by resource manager'
    for node in exclude:
      if node != '' and node not in downnodes:
        downnodes[node] = 'Excluded by resource manager'
    return downnodes

  def get_scr_end_time(self):
    resp = RPC(self.flux, "resource.status").get()
    rset = ResourceSet(resp["R"])
    endtime = 0
    try:
      endtime = int(rset.expiration)
    except:
      pass
    return endtime
