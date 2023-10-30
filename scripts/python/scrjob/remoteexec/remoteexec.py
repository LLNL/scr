import os
from subprocess import TimeoutExpired

from scrjob import config


class RemoteExec(object):
    """Execute command on remote compute nodes (without MPI launcher).

    Returns the stdout, stderr, and return code from each node.
    """

    def __init__(self):
        pass

    def rexec(self, argv, nodes, jobenv):
        """Executes command in argv on nodes.

        argv is a list of arguments representing the command.
        nodes is a list of node names on which to execute the command.

        Returns
        -------
        list
          The return value is: [output, returncode],
          where output is a list: [stdout, stderr],
          so the full return value is then: [[stdout, stderr], returncode].
        """
        pass
