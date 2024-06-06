import os

from scrjob.common import runproc, choose_bindir


class SCRRetriesHalt:

    def __init__(self, prefix, verbose=False):
        self.bindir = choose_bindir()
        self.prefix = prefix  # path to SCR_PREFIX
        self.verbose = verbose
        self.exe = os.path.join(self.bindir,
                                "scr_retries_halt") + " --dir " + str(prefix)

    # return list of output datasets
    def check(self):
        out, rc = runproc(self.exe, getstdout=True, verbose=self.verbose)
        if rc == 0:
            return out.rstrip()
