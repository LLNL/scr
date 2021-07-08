#! /usr/bin/env python3

# scr_environment.py
# This script defines the definition of the SCR_Env class
# SCR_Env contains general values from the environment

import os
import scr_const
from scr_common import scr_prefix

class SCR_Env:
  def __init__(self):
    # we can keep a reference to the other objects
    self.param = None
    self.launcher = None
    self.resmgr = None
    # initialize the infos
    self.conf = {}
    self.conf['prefix'] = scr_prefix()
    self.conf['nodes_file'] = scr_const.X_BINDIR+'/scr_nodes_file'
    self.conf['user'] = os.environ.get('USER')
    self.conf['nodes'] = os.environ.get('SCR_NODELIST')

  # set the nodelist (called if the environment variable wasn't set)
  def set_nodelist(self,nodelist):
    self.conf['nodes'] = nodelist
    os.environ['SCR_NODELIST'] = nodelist

  # set the prefix
  def set_prefix(self,prefix):
    self.conf['prefix'] = prefix

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'], '--dir', self.conf['prefix']]
    out, returncode = runproc(argv=argv, getstdout=True)
    if returncode == 0:
      return int(out)
    return 0 # print(err)
