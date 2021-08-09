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
    if 'pdsh_echo' not in self.nodetests.tests:
      self.nodetests.tests.append('pdsh_echo')

  # get LSF jobid
  def getjobid(self):
    return os.environ.get('LSB_JOBID')

  # get node list
  def get_job_nodes(self):
    hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
    if hostfile is not None:
      try:
        # make a list from the set -> make a set from the list -> file.readlines().rstrip('\n')
        # get a list of lines without newlines and skip the first line
        lines = []
        with open(hostfile, 'r') as f:
          lines = [line.strip() for line in f.readlines()][1:]
        if len(lines) == 0:
          raise ValueError('Hostfile empty')

        # get a set of unique hostnames, convert list to set and back
        hostlist = list(set(lines[1:]))
        hostlist = self.compress_hosts(hostlist)
        return hostlist
      except Exception as e:
        # failed to read file
        print('ERROR: LSF.get_job_nodes')
        print(e)
    return None

    # fall back to try LSB_HOSTS
    hosts = os.environ.get('LSB_HOSTS')
    if hosts is not None:
      hosts = hosts.split(' ')
      hosts = list(set(hosts[1:]))
      hosts = self.compress_hosts(hosts)
    return hosts

  def get_downnodes(self):
    # TODO : any way to get list of down nodes in LSF?
    return {}

  def get_scr_end_time(self):
    # run bjobs to get time remaining in current allocation
    bjobs, rc = runproc("bjobs -o time_left", getstdout=True)
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
      #print(line)

      # get current secs since epoch
      secs_now = int(time())

      # compute seconds left in job
      hours = int(pieces[2])
      mins = int(pieces[3])
      secs_remaining = ((hours * 60) + mins) * 60

      secs = secs_now + secs_remaining
      return secs

    # had a problem executing bjobs command
    return 0
