#! /usr/bin/env python3

import os

from pyfe import scr_const
from pyfe.scr_common import runproc


class SCRIndex:
  def __init__(self, prefix):
    self.bindir = scr_const.X_BINDIR  # path to SCR bin directory
    self.prefix = prefix  # path to SCR_PREFIX
    self.exe = os.path.join(self.bindir, "scr_index") + " --prefix " + prefix

  # make named dataset as current
  def current(self, name):
    rc = runproc(self.exe + " --current " + name)[1]
    return (rc == 0)

  # run build command to inspect and rebuild dataset files
  def build(self, dset):
    rc = runproc(self.exe + " --build " + dset)[1]
    return (rc == 0)
