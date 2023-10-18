#! /usr/bin/env python3

import os

from scrjob import scr_const
from scrjob.scr_common import runproc
from scrjob.scr_common import choose_bindir


class SCRIndex:

    def __init__(self, prefix, verbose=False):
        self.bindir = choose_bindir()
        self.prefix = prefix  # path to SCR_PREFIX
        self.verbose = verbose
        self.exe = os.path.join(self.bindir,
                                "scr_index") + " --prefix " + str(prefix)

    # capture and return verbatim output from the --list command
    def list(self):
        output, rc = runproc(self.exe + " --list",
                             getstdout=True,
                             verbose=self.verbose)
        if rc == 0:
            return output

    # make named dataset as current
    def current(self, name):
        rc = runproc(self.exe + " --current " + str(name),
                     verbose=self.verbose)[1]
        return (rc == 0)

    # add dataset to index file (must already exist)
    def add(self, dset):
        rc = runproc(self.exe + " --add " + str(dset), verbose=self.verbose)[1]
        return (rc == 0)

    # run build command to inspect and rebuild dataset files
    def build(self, dset):
        rc = runproc(self.exe + " --build " + str(dset),
                     verbose=self.verbose)[1]
        return (rc == 0)
