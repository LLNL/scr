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

        argv is a list of arguments representing the command. nodes is a
        list of node names on which to execute the command.

        Returns RemoteExecResult with stdout, stderr, and rc for each
        node.
        """
        pass


class RemoteExecResult(object):
    """Represents output from a RemoteExec operation.

    Contains stdout, stderr, and return code for each node.
    """

    def __init__(self, argv, nodes):
        # make a copy of the command and nodeset
        self._argv = argv.copy()
        self._nodes = nodes.copy()

        self.stdouts = dict()
        self.stderrs = dict()
        self.rcs = dict()
        for node in nodes:
            self.stdouts[node] = ""
            self.stderrs[node] = ""
            self.rcs[node] = None

    def argv(self):
        return self._argv

    def nodes(self):
        return self._nodes

    def append_stdout(self, node, val):
        if node in self.stdouts:
            self.stdouts[node] += val

    def append_stderr(self, node, val):
        if node in self.stderrs:
            self.stderrs[node] += val

    def set_rc(self, node, rc):
        if node in self.rcs:
            self.rcs[node] = rc

    def stdout(self, node):
        if node in self.stdouts:
            return self.stdouts[node]

    def stderr(self, node):
        if node in self.stderrs:
            return self.stderrs[node]

    def rc(self, node):
        if node in self.rcs:
            return self.rcs[node]
