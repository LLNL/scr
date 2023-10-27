import os
from time import sleep

from scrjob import config
from scrjob.scr_common import runproc, pipeproc
from scrjob.launchers import JobLauncher


class SRUN(JobLauncher):

    def __init__(self, launcher='srun'):
        super(SRUN, self).__init__(launcher=launcher)

    # a command to run immediately before prerun is ran
    # NOP srun to force every node to run prolog to delete files from cache
    # TODO: remove this if admins find a better place to clear cache
    def prepare_prerun(self):
        # NOP srun to force every node to run prolog to delete files from cache
        # TODO: remove this if admins find a better place to clear cache
        argv = ['srun', '/bin/hostname']  # ,'>','/dev/null']
        runproc(argv=argv)

    # returns the process and PID of the launched process
    # as returned by runproc(argv=argv, wait=False)
    def launch_run_cmd(self, up_nodes='', down_nodes='', launcher_args=[]):
        if type(launcher_args) is str:
            launcher_args = launcher_args.split()
        if len(launcher_args) == 0:
            return None, None
        argv = [self.launcher]
        if len(down_nodes) > 0:
            argv.extend(['--exclude', down_nodes])
        argv.extend(launcher_args)

        # The Popen.terminate() seems to work for srun
        if config.USE_JOBLAUNCHER_KILL != '1':
            return runproc(argv=argv, wait=False)
        proc = runproc(argv=argv, wait=False)[0]

        ### TODO: If we allow this to be toggled, we have to get the user and allocid below!
        jobstepid = self.jobstep_id(pid=proc.pid)
        if jobstepid is not None:
            return proc, jobstepid
        else:
            return proc, proc

    # perform a generic pdsh / clustershell command
    # returns [ [ stdout, stderr ] , returncode ]
    def parallel_exec(self, argv=[], runnodes=[]):
        if len(argv) == 0:
            return [['', ''], 0]
        if self.clustershell_task != False:
            return self.clustershell_exec(argv=argv, runnodes=runnodes)
        runnodes = ",".join(runnodes)
        pdshcmd = [
            config.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', runnodes,
            'srun', '-n', '1', '-N', '1', '-w', '%h'
        ]
        pdshcmd.extend(argv)
        return runproc(argv=pdshcmd, getstdout=True, getstderr=True)

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
    def kill_jobstep(self, jobstep=None):
        # it looks like the Popen.terminate is working with srun
        if type(jobstep) is str:
            runproc(argv=['scancel', jobstep])
        else:
            super().kill_jobstep(jobstep)
