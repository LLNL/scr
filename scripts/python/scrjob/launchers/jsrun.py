from time import sleep

from scrjob import config
from scrjob.launchers import JobLauncher
from scrjob.common import runproc


class JSRUN(JobLauncher):

    def __init__(self, launcher='jsrun'):
        super(JSRUN, self).__init__(launcher=launcher)
        self.jsrun_exe = 'jsrun'

    # returns the subprocess.Popen object as left and right elements of a tuple,
    # as returned by runproc(argv=argv, wait=False)
    def launch_run(self, args, nodes=[], down_nodes=[]):
        argv = [self.jsrun_exe]
        if down_nodes:
            down_str = ','.join(down_nodes)
            argv.extend(['--exclude_hosts', down_str])
        argv.extend(args)

        # it looks like the Popen.terminate is working with jsrun
        if config.USE_JOBLAUNCHER_KILL != '1':
            return runproc(argv=argv, wait=False)

        proc = runproc(argv=argv, wait=False)[0]
        jobstepid = self.jobstep_id()

        if jobstepid != '-1':
            return proc, jobstepid
        else:
            return proc, proc

    # query jslist for the most recent jobstep in current allocation
    def jobstep_id(self):
        # allow launched job to show in jslist
        sleep(10)

        # track the highest number running job
        jobstepid = -1

        # get the output of 'jslist'
        output = runproc(['jslist'], getstdout=True)[0]
        for line in output.split('\n'):
            if 'Running' not in line:
                continue
            line = line.split()

            # Format:
            # ID ParentID nrs CPUs/rs GPUs/rs ExitStatus Status
            #  2        0   2       1       0          0  Running
            if int(line[0]) > jobstepid:
                jobstepid = int(line[0])
        return str(jobstepid)

    # Only use jskill to kill the jobstep if desired and jobstep_id was successful
    def kill_run(self, jobstep=None):
        # it looks like the Popen.terminate is working with jsrun
        if type(jobstep) is str:
            runproc(argv=['jskill', jobstep])
        else:
            super().kill_run(jobstep)
