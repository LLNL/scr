#! /usr/bin/env python3

# pbsalps.py
# cray xt
# PBSALPS is a subclass of ResourceManager

import os, re
from pyfe import scr_const
from pyfe.scr_common import runproc, pipeproc
from pyfe.resmgr import nodetests, ResourceManager


class PBSALPS(ResourceManager):
  # init initializes vars from the environment
  def __init__(self, env=None):
    super(PBSALPS, self).__init__(resmgr='PBSALPS')
    if 'ping' not in self.nodetests.tests:
      self.nodetests.tests.append('ping')
    if 'dir_capacity' not in self.nodetests.tests:
      self.nodetests.tests.append('dir_capacity')

  # get job id, setting environment flag here
  def getjobid(self):
    if self.jobid is not None:
      return self.jobid
    # val may be None
    return os.environ.get('PBS_JOBID')

  # get node list
  def get_job_nodes(self):
    val = os.environ.get('PBS_NUM_NODES')
    if val is not None:
      cmd = "aprun -n " + val + " -N 1 cat /proc/cray_xt/nid"  # $nidfile
      out = runproc(cmd, getstdout=True)[0]
      nodearray = out.split('\n')
      if len(nodearray) > 0:
        if nodearray[-1] == '\n':
          nodearray = nodearray[:-1]
        if len(nodearray) > 0:
          if nodearray[-1].startswith('Application'):
            nodearray = nodearray[:-1]
          shortnodes = self.compress_hosts(nodearray)
          return shortnodes
    return None

  def get_downnodes(self):
    downnodes = {}
    snodes = self.get_job_nodes()
    if snodes is not None:
      snodes = self.expand_hosts(snodes)
      for node in snodes:
        out, returncode = runproc("xtprocadmin -n " + node, getstdout=True)
        #if returncode==0:
        resarray = out.split('\n')
        answerarray = resarray[1].split(' ')
        answer = answerarray[4]
        if 'down' in answer:
          downnodes[node] = 'Reported down by resource manager'
    return downnodes
