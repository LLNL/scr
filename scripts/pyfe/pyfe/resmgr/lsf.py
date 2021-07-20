#! /usr/bin/env python3

# lsf.py
# LSF is a subclass of ResourceManager

import os, re
from time import time

from pyfe import scr_const
from pyfe.scr_common import runproc, pipeproc
from pyfe.resmgr import nodetests, ResourceManager

class LSF(ResourceManager):
  # init initializes vars from the environment
  def __init__(self):
    super(LSF, self).__init__(resmgr='LSF')

  # get LSF jobid
  def getjobid(self):
    return os.environ.get('LSB_JOBID')

  # this doesn't really apply for LSF
  def get_jobstep_id(self,user='',pid=-1):
    return -1

  # get node list
  def get_job_nodes(self):
    hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
    if hostfile is not None:
      try:
        # make a list from the set -> make a set from the list -> file.readlines().rstrip('\n')
        # get a list of lines without newlines and skip the first line
        lines = []
        with open(hostfile,'r') as f:
          lines = [line.strip() for line in f.readlines()][1:]
        if len(lines) == 0:
          raise ValueError('Hostfile empty')

        # get a set of unique hostnames, convert list to set and back
        hostlist = self.compress_hosts(lines)
        return hostlist
      except:
        # failed to read file
        pass

    # fall back to try LSB_HOSTS
    hosts = os.environ.get('LSB_HOSTS')
    if hosts is not None:
      hosts = hosts.split(' ')
      hosts = hosts[1:]
      hosts = self.compress_hosts(hosts)
    return hosts

  def get_downnodes(self):
    # TODO : any way to get list of down nodes in LSF?
    return None

  def scr_kill_jobstep(self,jobid=-1):
    if jobid == -1:
      print('You must specify the job step id to kill.')
      return 1
    return runproc(argv=['bkill', '-s', 'KILL', str(jobid)])[1]

  def get_scr_end_time(self):
    # run bjobs to get time remaining in current allocation
    bjobs, rc = runproc(argv=['bjobs', '-o', 'time_left'], getstdout=True)
    if rc != 0:
      return 0

    # parse bjobs output
    lines = bjobs.split('\n')
    for line in lines:
      line = line.strip()

      # skip empty lines
      if len(line) == 0:
        continue

      # the following is printed if there is no limit
      #   bjobs -o 'time_left'
      #   TIME_LEFT
      #   -
      # look for the "-", in this case,
      # return -1 to indicate there is no limit
      if line.startswith('-'):
        # no limit
        return -1

      # the following is printed if there is a limit
      #   bjobs -o 'time_left'
      #   TIME_LEFT
      #   0:12 L
      # look for a line like "0:12 L",
      # avoid matching the "L" since other characters can show up there
      pieces = re.split(r'(^\s*)(\d+):(\d+)\s+', line)
      if len(pieces) < 3:
        continue
      print(line)

      # get current secs since epoch
      secs_now = int(time())

      # compute seconds left in job
      hours = int(pieces[2])
      mins  = int(pieces[3])
      secs_remaining = ((hours * 60) + mins) * 60

      secs = secs_now + secs_remaining
      return secs

    # had a problem executing bjobs command
    return 0

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    unavailable = nodetests.list_resmgr_down_nodes(nodes=nodes, resmgr_nodes=self.expand_hosts(self.get_downnodes()))
    nextunavail = nodetests.list_pdsh_fail_echo(nodes=nodes, nodes_string=self.compress_hosts(nodes), launcher=scr_env.launcher)
    unavailable.update(nextunavail)
    if scr_env is not None and scr_env.param is not None:
      exclude_nodes = self.expand_hosts(scr_env.param.get('SCR_EXCLUDE_NODES'))
      nextunavail = nodetests.list_param_excluded_nodes(nodes=self.expand_hosts(nodes), exclude_nodes=exclude_nodes)
      unavailable.update(nextunavail)
    return unavailable
