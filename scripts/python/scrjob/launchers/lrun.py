from scrjob import config
from scrjob.launchers import JobLauncher
from scrjob.common import runproc


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
