import os

from scrjob.common import runproc, choose_bindir


class SCRFlushFile:

    def __init__(self, prefix, verbose=False):
        self.bindir = choose_bindir()
        self.prefix = prefix  # path to SCR_PREFIX
        self.verbose = verbose
        self.exe = os.path.join(self.bindir,
                                "scr_flush_file") + " --dir " + str(prefix)

    # return list of output datasets
    def list_dsets_output(self):
        dsets, rc = runproc(self.exe + " --list-output",
                            getstdout=True,
                            verbose=self.verbose)
        if rc == 0:
            return dsets.split()
        return []

    # return list of checkpoints
    def list_dsets_ckpt(self, before=None):
        cmd = self.exe + " --list-ckpt"
        if before:
            cmd = cmd + " --before " + str(before)
        dsets, rc = runproc(cmd, getstdout=True, verbose=self.verbose)
        if rc == 0:
            return dsets.split()
        return []

    # determine whether this dataset needs to be flushed
    def need_flush(self, d):
        rc = runproc(self.exe + " --need-flush " + str(d),
                     verbose=self.verbose)[1]
        return (rc == 0)

    # return name of specified dataset
    def name(self, d):
        name, rc = runproc(self.exe + " --name " + str(d),
                           getstdout=True,
                           verbose=self.verbose)
        if rc == 0:
            return name.rstrip()

    # return the latest dataset id, or None
    def latest(self):
        dset, rc = runproc(self.exe + " --latest",
                           getstdout=True,
                           verbose=self.verbose)
        if rc == 0:
            return dset.rstrip()

    # return the location string for the given dataset id
    def location(self, d):
        dset, rc = runproc(self.exe + " --location " + str(d),
                           getstdout=True,
                           verbose=self.verbose)
        if rc == 0:
            return dset.rstrip()

    # resume a transfer of specified dataset id, used in poststage
    def resume(self, d):
        dset, rc = runproc(self.exe + " --resume --name " + str(d),
                           verbose=self.verbose)[1]
        return (rc == 0)

    # create summary file for specified dataset id, used in prestage
    def write_summary(self, d):
        dset, rc = runproc(self.exe + " --summary --name " + str(d),
                           verbose=self.verbose)[1]
        return (rc == 0)
