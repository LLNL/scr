#! /usr/bin/env python3

# resmgr/flux.py
# FLUX is a subclass of ResourceManager

import os, re
import datetime

from pyfe import scr_const
from pyfe.resmgr import nodetests, ResourceManager

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
    super(FLUX, self).__init__(resmgr='FLUX')
    try:
      self.flux = flux.Flux()
    except:
      raise ImportError('Error importing flux, ensure that the flux daemon is running.')

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
    return 0
