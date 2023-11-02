import os
from time import sleep

from scrjob import config
from scrjob.common import runproc
from scrjob.launchers import JobLauncher


class SRUN(JobLauncher):

    def __init__(self, launcher='srun'):
        super(SRUN, self).__init__(launcher=launcher)
        self.srun_exe = 'srun'

    # returns the process and PID of the launched process
    # as returned by runproc(argv=argv, wait=False)
    def launch_run(self, args, nodes=[], down_nodes=[]):
        argv = [self.srun_exe]
        if down_nodes:
            down_str = ','.join(down_nodes)
            argv.extend(['--exclude', down_str])
        argv.extend(args)

        # The Popen.terminate() seems to work for srun
        if config.USE_JOBLAUNCHER_KILL != '1':
            return runproc(argv=argv, wait=False)

        proc = runproc(argv=argv, wait=False)[0]

        # TODO: If we allow this to be toggled, we have to get the user and allocid below!
        jobstepid = self.jobstep_id(pid=proc.pid)
        if jobstepid is not None:
            return proc, jobstepid
        else:
            return proc, proc

    # query SLURM for the most recent jobstep in current allocation
    def jobstep_id(self, pid):
        ### TODO: If we allow this to be toggled, we have to get the user and allocid!
        ### Or, the command ran could possibly be changed . . .
        user = os.environ.get('USER')
        allocid = os.environ.get('SLURM_JOBID')
        if user == '' or allocid == '':
            return None

        # allow launched job to show in squeue
        sleep(10)

        # get job steps for this user and job, order by decreasing job step
        # so first one should be the one we are looking for
        #   squeue -h -s -u $user -j $jobid -S "-i"
        # -h means print no header, so just the data in this order:
        # STEPID         NAME PARTITION     USER      TIME NODELIST
        cmd = "squeue -h -s -u " + user + " -j " + allocid + " -S \"-i\""
        output = runproc(cmd, getstdout=True)[0]
        output = re.search('\d+', output)
        try:
            jobstepid = output[0]
            return jobstepid
        except:
            return None

    # Only use scancel to kill the jobstep if desired and jobstep_id was successful
    def kill_run(self, jobstep=None):
        # it looks like the Popen.terminate is working with srun
        if type(jobstep) is str:
            runproc(argv=['scancel', jobstep])
        else:
            super().kill_run(jobstep)
