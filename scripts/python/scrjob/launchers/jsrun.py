from time import sleep

from scrjob import scr_const
from scrjob.launchers import JobLauncher
from scrjob.scr_common import runproc, pipeproc


class JSRUN(JobLauncher):

    def __init__(self, launcher='jsrun'):
        super(JSRUN, self).__init__(launcher=launcher)

    # returns the subprocess.Popen object as left and right elements of a tuple,
    # as returned by runproc(argv=argv, wait=False)
    def launch_run_cmd(self, up_nodes='', down_nodes='', launcher_args=[]):
        if type(launcher_args) is str:
            launcher_args = launcher_args.split()
        if len(launcher_args) == 0:
            return None, None
        argv = [self.launcher]
        if down_nodes != '':
            argv.append('--exclude_hosts ' + down_nodes)
        argv.extend(launcher_args)

        # it looks like the Popen.terminate is working with jsrun
        if scr_const.USE_JOBLAUNCHER_KILL != '1':
            return runproc(argv=argv, wait=False)
        proc = runproc(argv=argv, wait=False)[0]
        jobstepid = self.jobstep_id()
        if jobstepid != '-1':
            return proc, jobstepid
        else:
            return proc, proc

    # perform a generic pdsh / clustershell command
    # returns [ [ stdout, stderr ] , returncode ]
    def parallel_exec(self, argv=[], runnodes=''):
        if len(argv) == 0:
            return [['', ''], 0]
        if self.clustershell_task != False:
            return self.clustershell_exec(argv=argv, runnodes=runnodes)
        pdshcmd = [
            scr_const.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', runnodes
        ]
        pdshcmd.extend(argv)
        return runproc(argv=pdshcmd, getstdout=True, getstderr=True)

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
    def kill_jobstep(self, jobstep=None):
        # it looks like the Popen.terminate is working with jsrun
        if type(jobstep) is str:
            runproc(argv=['jskill', jobstep])
        else:
            super().kill_jobstep(jobstep)
