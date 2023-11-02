import os

from scrjob import config
from scrjob.launchers import JobLauncher
from scrjob.common import runproc


class MPIRUN(JobLauncher):

    def __init__(self, launcher='mpirun'):
        super(MPIRUN, self).__init__(launcher=launcher)
        self.mpirun_exe = 'mpirun'

    # returns the subprocess.Popen object as left and right elements of a tuple,
    # as returned by runproc(argv=argv, wait=False)
    def launch_run(self, args, nodes=[], down_nodes=[]):
        # split the node string into a node per line
        up_nodes = '/n'.join(nodes)
        try:
            # need to first ensure the directory exists
            basepath = '/'.join(self.hostfile.split('/')[:-1])
            os.makedirs(basepath, exist_ok=True)

            with open(self.hostfile, 'w') as f:
                f.write(up_nodes)

            argv = [self.mpirun_exe, '--hostfile', self.hostfile]
            argv.extend(args)
            return runproc(argv=argv, wait=False)
        except Exception as e:
            print(e)
            print(
                'scr_mpirun: Error writing hostfile and creating launcher command'
            )
            print('launcher file: \"' + self.hostfile + '\"')
            return None, None
