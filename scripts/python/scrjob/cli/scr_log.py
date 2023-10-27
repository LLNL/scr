import os

from scrjob.common import runproc, choose_bindir


class SCRLog:

    def __init__(self,
                 prefix,
                 jobid,
                 jobname=None,
                 user=None,
                 jobstart=None,
                 verbose=False):
        self.bindir = choose_bindir()
        self.prefix = prefix  # path to SCR_PREFIX
        self.user = user
        self.jobid = jobid
        self.jobname = jobname
        self.jobstart = jobstart
        self.verbose = verbose

        self.exe_event = os.path.join(self.bindir, "scr_log_event")
        self.eve_transfer = os.path.join(self.bindir, "scr_log_transfer")

    # return list of output datasets
    def event(self,
              event_type,
              dset=None,
              name=None,
              start=None,
              secs=None,
              note=None):
        argv = [self.exe_event, '-p', self.prefix]

        if self.user is not None:
            argv.extend(['-u', str(self.user)])

        if self.jobid is not None:
            argv.extend(['-i', str(self.jobid)])

        if self.jobname is not None:
            argv.extend(['-j', str(self.jobname)])

        if self.jobstart is not None:
            argv.extend(['-s', str(self.jobstart)])

        if event_type is not None:
            argv.extend(['-T', str(event_type)])

        if dset is not None:
            argv.extend(['-D', str(dset)])

        if name is not None:
            argv.extend(['-n', str(name)])

        if start is not None:
            argv.extend(['-S', str(start)])

        if secs is not None:
            argv.extend(['-L', str(secs)])

        if note is not None:
            argv.extend(['-N', str(note)])

        rc = runproc(argv, verbose=self.verbose)[1]
        return (rc == 0)
