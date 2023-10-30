import os

from scrjob import config
from scrjob.launchers import JobLauncher
from scrjob.common import runproc


class MPIRUN(JobLauncher):

    def __init__(self, launcher='mpirun'):
        super(MPIRUN, self).__init__(launcher=launcher)

    # returns the subprocess.Popen object as left and right elements of a tuple,
    # as returned by runproc(argv=argv, wait=False)
    def launch_run_cmd(self, up_nodes='', down_nodes='', launcher_args=[]):
        if type(launcher_args) is str:
            launcher_args = launcher_args.split()
        if len(launcher_args) == 0:
            return None, None

        # split the node string into a node per line
        up_nodes = '/n'.join(up_nodes.split(','))
        try:
            # need to first ensure the directory exists
            basepath = '/'.join(self.hostfile.split('/')[:-1])
            os.makedirs(basepath, exist_ok=True)
            with open(self.hostfile, 'w') as usehostfile:
                usehostfile.write(up_nodes)
            argv = [self.launcher, '--hostfile', self.hostfile]
            argv.extend(launcher_args)
            return runproc(argv=argv, wait=False)
        except Exception as e:
            print(e)
            print(
                'scr_mpirun: Error writing hostfile and creating launcher command'
            )
            print('launcher file: \"' + self.hostfile + '\"')
            return None, None
