#! /usr/bin/env python3

# scr_environment.py
# This script defines the definition of the SCR_Env class
# SCR_Env contains general values from the environment

import os
from pyfe import scr_const
from pyfe.scr_common import scr_prefix, runproc


class SCR_Env:
  def __init__(self, prefix=None):
    # we can keep a reference to the other objects
    self.param = None
    self.launcher = None
    self.resmgr = None

    # record SCR_PREFIX directory, default to scr_prefix if not specified
    if prefix is None:
      prefix = scr_prefix()
    self.prefix = prefix

    # initialize the infos
    bindir = scr_const.X_BINDIR
    self.nodes_file = os.path.join(bindir, 'scr_nodes_file')

  def get_user(self):
    return os.environ.get('USER')

  def get_scr_nodelist(self):
    return os.environ.get('SCR_NODELIST')

  # return path to $SCR_PREFIX
  def get_prefix(self):
    return self.prefix

  # return path to $SCR_PREFIX/.scr
  def dir_scr(self):
    return os.path.join(self.prefix, '.scr')

  # given a dataset id, return dataset directory within prefix
  # ${SCR_PREFIX}/.scr/scr.dataset.<id>
  def dir_dset(self, d):
    return os.path.join(self.dir_scr(), 'scr.dataset.' + str(d))

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.nodes_file, '--dir', self.prefix]
    out, returncode = runproc(argv=argv, getstdout=True)
    if returncode == 0:
      return int(out)
    return 0  # print(err)
