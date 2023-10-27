from scrjob import config
from scrjob.launchers import JobLauncher
from scrjob.scr_common import runproc, pipeproc


class LRUN(JobLauncher):

    def __init__(self, launcher='lrun'):
        super(LRUN, self).__init__(launcher=launcher)

    # returns the subprocess.Popen object as left and right elements of a tuple,
    # as returned by runproc(argv=argv, wait=False)
    def launch_run_cmd(self, up_nodes='', down_nodes='', launcher_args=[]):
        if type(launcher_args) is str:
            launcher_args = launcher_args.split()
        if len(launcher_args) == 0:
            return None, None
        argv = [self.launcher]
        if len(down_nodes) > 0:
            argv.append('--exclude_hosts=' + down_nodes)
        argv.extend(launcher_args)
        return runproc(argv=argv, wait=False)

    # perform a generic pdsh / clustershell command
    # returns [ [ stdout, stderr ] , returncode ]
    def parallel_exec(self, argv=[], runnodes=[]):
        if len(argv) == 0:
            return [['', ''], 0]
        if self.clustershell_task != False:
            return self.clustershell_exec(argv=argv, runnodes=runnodes)
        runnodes = ",".join(runnodes)
        pdshcmd = [config.PDSH_EXE, '-f', '256', '-S', '-w', runnodes]
        pdshcmd.extend(argv)
        return runproc(argv=pdshcmd, getstdout=True, getstderr=True)
