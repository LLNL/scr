from scrjob import config
from scrjob.common import runproc
from scrjob.remoteexec import RemoteExec


class Pdsh(RemoteExec):

    def __init__(self, launcher=None):
        self.launcher = launcher

    def rexec(self, argv, nodes, jobenv):
        """Run command on a set of nodes using pdsh."""

        nodestr = ",".join(nodes)
        cmd = [config.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', nodestr]

        # Some systems require one to use the job launcher
        # to start any process on the compute nodes.
        # pdsh replaces "%h" with the target hostname.
        if self.launcher == 'srun':
            cmd.extend(['srun', '-n', '1', '-N', '1', '-w', '%h'])
        elif self.launcher == 'aprun':
            cmd.extend(['aprun', '-n', '1', 'L', '%h'])

        cmd.extend(argv)

        output, rc = runproc(argv=cmd, getstdout=True, getstderr=True)
        return output, rc
