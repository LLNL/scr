import os

from scrjob.common import runproc, choose_bindir


class SCRHaltFile:

    def __init__(self, prefix, verbose=False):
        self.bindir = choose_bindir()
        self.prefix = prefix  # path to SCR_PREFIX
        self.verbose = verbose
        self.exe = os.path.join(self.bindir, "scr_halt_cntl")

        self.fname = os.path.join(self.prefix, '.scr', 'halt.scr')
        self.conditions = []

    def remove(self):
        """Remove any halt file."""
        os.remove(self.fname)

    def set_checkpoints(self, checkpoints):
        """Halt after X checkpoints."""
        self.conditions.append('-c')
        self.conditions.append(str(checkpoints))

    def set_before(self, secs):
        """Halt before time (when within SCR_HALT_SECONDS of given time)"""
        self.conditions.append('-b')
        self.conditions.append(str(secs))

    def set_after(self, secs):
        """Halt after time."""
        self.conditions.append('-a')
        self.conditions.append(str(secs))

    def set_seconds(self, secs):
        """Set (reset) SCR_HALT_SECONDS."""
        self.conditions.append('-s')
        self.conditions.append(str(secs))

    def set_halted(self):
        """Halt job on next opportunity."""
        self.conditions.append('-r')
        self.conditions.append('JOB_HALTED')

    def set_list(self):
        """Return current values set in halt file."""
        self.conditions.append('-l')

    def unset_checkpoints(self):
        self.conditions.append('-xc')

    def unset_before(self):
        self.conditions.append('-xb')

    def unset_after(self):
        self.conditions.append('-xa')

    def unset_seconds(self):
        self.conditions.append('-xs')

    def unset_reason(self):
        self.conditions.append('-xr')

    # return list of output datasets
    def execute(self):
        cmd = [self.exe, '-f', self.fname]

        # halt job with JOB_HALTED reason if not given other options
        if not self.conditions:
            self.set_halted()
        cmd.extend(self.conditions)

        output, rc = runproc(cmd,
                             getstdout=True,
                             getstderr=True,
                             verbose=self.verbose)
        if rc != 0:
            # raise an exception on error
            # TODO: want to return this stderr via exception?
            if output is not None:
                print(output[0].strip())
            raise RuntimeError(
                'scr_halt: ERROR: Failed to update halt file: ' + self.fname)

        # return stdout from command
        if output is not None:
            return output[0].strip()
