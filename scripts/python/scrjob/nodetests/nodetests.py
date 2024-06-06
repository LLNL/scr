import os

from scrjob import config

from scrjob.nodetests import (
    SCRExcludeNodes,
    ResMgrDown,
    Ping,
    Echo,
    DirCapacity,
)


class NodeTests(object):

    def __init__(self):
        # TODO: add tests based on config

        # Get the list of test names to perform.
        scr_node_tests = os.environ.get('SCR_NODE_TESTS')
        if scr_node_tests is not None:
            # get the list of tests from $SCR_NODE_TESTS
            tests = scr_node_tests.split(',')
        elif config.SCR_NODE_TESTS != '':
            # otherwise get the list of tests from config.SCR_NODE_TESTS
            tests = config.SCR_NODE_TESTS.split(',')
        elif config.SCR_NODE_TESTS_FILE != '':
            # Otherwise read the list of tests from the file given in SCR_NODE_TESTS_FILE.
            # Tests can be listed on multiple lines or a single line separated by commas.
            # Empty lines are skipped.
            tests = []
            fname = config.SCR_NODE_TESTS_FILE
            with open(fname, 'r') as f:
                for line in f.readlines():
                    line = line.strip()
                    if line != '':
                        tests.extend(line.split(','))
        else:
            # Default set of tests if nothing is given.
            tests = ['ping', 'echo', 'dir_capacity']

        # build list of tests to be executed according to list
        self.tests = []

        # nodes listed in SCR_EXCLUDE_NODES
        self.tests.append(SCRExcludeNodes())

        # nodes listed by resource manager to be down
        self.tests.append(ResMgrDown())

        for name in tests:
            if name == 'ping':
                self.tests.append(Ping())
            elif name == 'echo':
                self.tests.append(Echo())
            elif name == 'dir_capacity':
                self.tests.append(DirCapacity())

    def _drop_failed(self, failed, nodes):
        # drop any failed nodes from the list
        # we do this to avoid running further tests on nodes already marked as down
        for node in failed.keys():
            if node in nodes:
                nodes.remove(node)

    def execute(self, nodes, jobenv, down=[]):
        """Return a dictionary of down nodes."""

        # This method returns a dictionary of unavailable nodes
        # keyed by node name with the reason as the value
        unavailable = {}

        # the tests modify the node to avoid re-testing nodes
        # that are identified to be down by earlier tests
        nodes = nodes.copy()

        # drop any nodes that we are told are down
        for node in down:
            if node in nodes:
                nodes.remove(node)
                unavailable[node] = 'Specified as down'

        # run tests against remaining nodes
        for t in self.tests:
            failed = t.execute(nodes, jobenv)
            self._drop_failed(failed, nodes)
            unavailable.update(failed)

        return unavailable
