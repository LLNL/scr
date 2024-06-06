from scrjob.cli import SCRFlushFile


class Watchdog:
    """This class attempts to detect hanging applications in order to avoid
    wasting allocations.

    Use of the Watchdog requires 3 configuration variables to be set:
    SCR_WATCHDOG=1               The watchdog must be enabled (set to '1')

    We must also have an expected time (in seconds) to check for existence of checkpoint files.
    For example:
    SCR_WATCHDOG_TIMEOUT=300     An expected time where we should see a new in-system checkpoint.
    SCR_WATCHDOG_TIMEOUT_PFS=900 An expected time where we should see a new write to the PFS.

    If the Watchdog is enabled, and timeouts are set, then we will monitor for progress
    following the launch of a jobstep.

    Normally, we would ask the Joblauncher class to wait until a launched jobstep terminates.
    The Watchdog will ask the Joblauncher class to wait with a timeout value.
    The Joblauncher will return 0 if the jobstep is no longer running, and 1 if it is running.
    Each time the Joblauncher indicates the jobstep is still running, we will check for progress.
    If no progress has been made, we will ask the Joblauncher to terminate the jobstep.
    If progress has been made, we will ask the Joblauncher to again wait with a timeout.
    """

    def __init__(self, prefix, jobenv):
        """The Watchdog class is instantiated once, before any jobstep is ever
        launched, if Watchdog is enabled.

        Set timeout values from the environment. Copy the reference to
        the Joblauncher from the JobEnv class. Instantiate an instance
        of SCRFlushFile using the provided prefix for later checking.
        """
        self.timeout = jobenv.param.get('SCR_WATCHDOG_TIMEOUT')
        self.timeout_pfs = jobenv.param.get('SCR_WATCHDOG_TIMEOUT_PFS')

        if self.timeout is not None and self.timeout_pfs is not None:
            self.timeout = int(self.timeout)
            self.timeout_pfs = int(self.timeout_pfs)
            self.launcher = jobenv.launcher
            self.scr_flush_file = SCRFlushFile(prefix)

    def watchfiles(self, proc, jobstep):
        """This is an internal method In this method the Watchdog loops,
        periodically checking for activity."""
        timeToSleep = self.timeout
        lastCheckpoint = None
        lastCheckpointLoc = None
        while True:
            # wait up to 'timeToSleep' to see if the process terminates normally
            (finished, success) = self.launcher.wait_run(proc,
                                                         timeout=timeToSleep)

            # when the wait returns zero the process is no longer running
            if finished:
                return 0

            # the process is still running, read flush file to get latest
            # dataset id and its location
            latestLoc = None
            latest = self.scr_flush_file.latest()
            if latest:
                latestLoc = self.scr_flush_file.location(latest)

            # If nothing has changed since we last checked,
            # assume the process is hanging and kill it.
            if latest == lastCheckpoint:
                if latestLoc == lastCheckpointLoc:
                    # print('time to kill')
                    break

            # The flush file has changed since we last checked.
            # Assume the process is still making progress.
            # Remember current values, set new timeout, and loop back to wait again.
            lastCheckpoint = latest
            lastCheckpointLoc = latestLoc
            if latestLoc == 'SYNC_FLUSHING':
                timeToSleep = self.timeout_pfs
            else:
                timeToSleep = self.timeout
        # forward progress not observed in an expected timeframe
        # kill the watched process and return
        self.launcher.kill_run(jobstep)
        return 0

    def watchproc(self, watched_process=None, jobstep=None):
        """Watchproc is the method called after launcher.launch_run()

        Parameters
        ----------
        watched_process - The reference needed for a Joblauncher to wait on a process.
        jobstep         - The reference needed for a Joblauncher to terminate a jobstep.

        Returns
        -------
        int
           0 - Indicates the jobstep is no longer running, regardless of reason for termination.
           1 - Indicates the Watchdog could not be initialized.
        """
        if watched_process is None or jobstep is None:
            print('watchdog: ERROR: No process to watch.')
            return 1

        # TODO: What to do if timeouts are not set? die? should we set default values?
        # for now die with error message
        if self.timeout is None or self.timeout_pfs is None:
            print(
                'Necessary environment variables not set: SCR_WATCHDOG_TIMEOUT and SCR_WATCHDOG_TIMEOUT_PFS'
            )
            # Returning 1 to scr_run to indicate watchdog did not start/complete
            return 1
        return self.watchfiles(proc=watched_process, jobstep=jobstep)
