from scrjob import config
from scrjob.launchers import JobLauncher
from scrjob.common import runproc


class LRUN(JobLauncher):

    def __init__(self, launcher='lrun'):
        super(LRUN, self).__init__(launcher=launcher)
        self.lrun_exe = 'lrun'

    # returns the subprocess.Popen object as left and right elements of a tuple,
    # as returned by runproc(argv=argv, wait=False)
    def launch_run(self, args, nodes=[], down_nodes=[]):
        argv = [self.lrun_exe]
        if down_nodes:
            argv.append('--exclude_hosts=' + ','.join(down_nodes))
        argv.extend(args)
        return runproc(argv=argv, wait=False)
