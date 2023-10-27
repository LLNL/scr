import os

from scrjob.common import runproc, choose_bindir


class SCRNodesFile:

    def __init__(self, prefix, verbose=False):
        self.bindir = choose_bindir()
        self.prefix = prefix  # path to SCR_PREFIX
        self.verbose = verbose
        self.exe = os.path.join(self.bindir,
                                "scr_nodes_file") + " --dir " + str(prefix)

    # return number of nodes used in last run
    # returns None if cannot be determined
    def last_num_nodes(self):
        out, rc = runproc(self.exe, getstdout=True, verbose=self.verbose)
        if rc == 0:
            return int(out)
