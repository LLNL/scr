#! /usr/bin/env python3

import os

from pyfe.scr_common import runproc

class Index:
  def __init__(self, bindir, prefix):
    self.bindir = bindir # path to SCR bin directory
    self.prefix = prefix # path to SCR_PREFIX
    self.exe = os.path.join(bindir, "scr_index") + " --prefix " + prefix

  # make named dataset as current
  def current(self, name):
    rc = runproc(self.exe + " --current " + name)
    return (rc == 0)

  # run build command to inspect and rebuild dataset files
  def build(self, dset):
    rc = runproc(self.exe + " --build " + dset)
    return (rc == 0)
