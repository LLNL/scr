#! /usr/bin/env python3

import os

from pyfe import scr_const
from pyfe.scr_common import runproc


class SCRIndex:
  def __init__(self, prefix, verbose=False):
    self.bindir = scr_const.X_BINDIR  # path to SCR bin directory
    self.prefix = prefix  # path to SCR_PREFIX
    self.verbose = verbose
    self.exe = os.path.join(self.bindir, "scr_index") + " --prefix " + str(prefix)

  # capture and return verbatim output from the --list command
  def list(self):
    output, rc = runproc(self.exe + " --list", getstdout=True, verbose=self.verbose)
    if rc == 0:
      return output

  # make named dataset as current
  def current(self, name):
    rc = runproc(self.exe + " --current " + str(name), verbose=self.verbose)[1]
    return (rc == 0)

  # add dataset to index file (must already exist)
  def add(self, dset):
    rc = runproc(self.exe + " --add " + str(dset), verbose=self.verbose)[1]
    return (rc == 0)

  # run build command to inspect and rebuild dataset files
  def build(self, dset):
    rc = runproc(self.exe + " --build " + str(dset), verbose=self.verbose)[1]
    return (rc == 0)
