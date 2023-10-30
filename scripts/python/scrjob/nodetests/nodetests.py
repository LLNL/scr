import os

from scrjob import config

from scrjob.nodetests import (
    Ping,
    Echo,
    DirCapacity,
)


class NodeTests(object):

    def __init__(self, jobenv):
        self.firstrun = True
        self.jobenv = jobenv

        # The tests which will be performed should be set either:
        # When self.nodetests is instantiated (in init of Nodetests):
        #   by constant list in config.py
        #   by file input, where the filename is specified in config.py
        # Or manually by adding test names to the self.nodetests.nodetests list
        # in your resource manager's init after super().__init__

        # TODO: add tests based on config
        # First try to read the environment variable
        tests = os.environ.get('SCR_NODE_TESTS')
        if tests is not None:
            tests = tests.split(',')
        elif config.SCR_NODE_TESTS != '':
            tests = config.SCR_NODE_TESTS.split(',')
        elif config.SCR_NODE_TESTS_FILE != '':
            tests = []
            fname = config.SCR_NODE_TESTS_FILE
            with open(fname, 'r') as f:
                for line in f.readlines():
                    line = line.strip()
                    if line != '':
                        tests.extend(line.split(','))
        else:
            tests = []

        # build list of tests to be executed according to list
        self.tests = []
        for name in tests:
            if name == 'ping':
                self.tests.append(Ping(jobenv))
            elif name == 'echo':
                self.tests.append(Echo(jobenv))
            elif name == 'dir_capacity':
                self.tests.append(DirCapacity(jobenv))

    def _drop_failed(self, failed, nodes):
        # drop any failed nodes from the list
        # we do this to avoid running further tests on nodes already marked as down
        for node in failed.keys():
            if node in nodes:
                nodes.remove(node)

    def execute(self, nodes):
        """Return a dictionary of down nodes"""

        # This method returns a dictionary of unavailable nodes
        # keyed by node name with the reason as the value
        unavailable = {}

        # nodes listed in SCR_EXCLUDE_NODES
        failed = self.user_excluded(nodes)
        self._drop_failed(failed, nodes)
        unavailable.update(failed)

        # nodes the resource manager thinks to be down
        failed = self.resmgr_down(nodes)
        self._drop_failed(failed, nodes)
        unavailable.update(failed)

        # run tests against remaining nodes
        for t in self.tests:
            failed = t.execute(nodes)
            self._drop_failed(failed, nodes)
            unavailable.update(failed)

        # allow alternate behavior on subsequent runs
        self.firstrun = False

        return unavailable

    def user_excluded(self, nodes):
        # nodes excluded via SCR_EXCLUDE_NODES
        failed = {}
        exclude_nodes = self.jobenv.param.get('SCR_EXCLUDE_NODES')
        nodelist = self.jobenv.resmgr.expand_hosts(exclude_nodes)
        for node in nodelist:
            failed[node] = 'User excluded via SCR_EXCLUDE_NODES'
        return failed

    def resmgr_down(self, nodes):
        # nodes the resource manager thinks is down
        failed = {}
        nodelist = self.jobenv.resmgr.down_nodes()
        for node in nodelist:
            failed[node] = nodelist[node]
        return failed
