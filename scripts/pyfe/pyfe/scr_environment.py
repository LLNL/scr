#! /usr/bin/env python3

# scr_environment.py
# This script defines the definition of the SCR_Env class
# SCR_Env contains general values from the environment

import os
from pyfe import scr_const
from pyfe.scr_common import scr_prefix, runproc

class SCR_Env:
  def __init__(self):
    # we can keep a reference to the other objects
    self.param = None
    self.launcher = None
    self.resmgr = None
    # initialize the infos
    self.prefix = scr_prefix()
    self.nodes_file = scr_const.X_BINDIR+'/scr_nodes_file'
    self.user = os.environ.get('USER')
    self.nodes = os.environ.get('SCR_NODELIST')

  def get_user(self):
    return self.user

  def get_scr_nodelist(self):
    return self.nodes

  # set the nodelist (called if the environment variable wasn't set)
  def set_nodelist(self,nodelist):
    self.nodes = nodelist
    os.environ['SCR_NODELIST'] = nodelist

  def get_prefix(self):
    return self.prefix

  # set the prefix
  def set_prefix(self,prefix):
    self.prefix = prefix

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.nodes_file, '--dir', self.prefix]
    out, returncode = runproc(argv=argv, getstdout=True)
    if returncode == 0:
      return int(out)
    return 0 # print(err)
