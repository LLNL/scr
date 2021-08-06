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
    return rset.nodelist

  def get_downnodes(self):
    resp = RPC(self.flux, "resource.status").get()
    rset = ResourceSet(resp["R"])
    offline = str(resp['offline'])
    exclude = str(resp['exclude'])
    if offline != '' and exclude != '':
      offline += ',' + exclude
    elif offline != '':
      return offline
    return exclude

  def get_scr_end_time(self):
    return 0

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,
                                  nodes=[],
                                  scr_env=None,
                                  free=False,
                                  cntldir_string=None,
                                  cachedir_string=None):
    resp = RPC(self.flux, "resource.status").get()
    rset = ResourceSet(resp["R"])
    offline = resp['offline']
    exclude = resp['exclude']
    unavailable = {}
    for rank in offline:
      unavailable[rank] = 'Determined offline by flux'
    for rank in exclude:
      unavailable[rank] = 'Excluded by flux'
    return unavailable
