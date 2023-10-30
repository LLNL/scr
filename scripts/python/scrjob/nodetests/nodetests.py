import os

from scrjob import config
from scrjob.common import runproc


class Nodetests:

    def __init__(self):
        # tests may perform different behavior during first vs subsequent runs
        self.firstrun = True

        # path to the ping executable
        self.pingexe = 'ping'

        # First try to read the environment variable
        self.tests = os.environ.get('SCR_NODE_TESTS')
        if self.tests is not None:
            self.tests = self.tests.split(',')
        elif config.SCR_NODE_TESTS != '':
            self.tests = config.SCR_NODE_TESTS.split(',')
        elif config.SCR_NODE_TESTS_FILE != '':
            try:
                self.tests = []
                with open(config.SCR_NODE_TESTS_FILE, 'r') as f:
                    for line in f.readlines():
                        line = line.strip()
                        if line != '':
                            self.tests.extend(line.split(','))
            except:
                print('nodetests.py: ERROR: Unable to open file ' +
                      config.SCR_NODE_TESTS_FILE)
        else:
            self.tests = []

    def _drop_failed(self, failed, nodes):
        # drop any failed nodes from the list
        # we do this to avoid running further tests on nodes already marked as down
        for node in failed.keys():
            if node in nodes:
                nodes.remove(node)

    def __call__(self, nodes=[], jobenv=None):
        if type(nodes) is str:
            nodes = nodes.split(',')

        # This method returns a dictionary of unavailable nodes
        #   keyed on node with the reason as the value
        unavailable = {}

        # mark any nodes to explicitly exclude via SCR_EXCLUDE_NODES
        failed = self.user_excluded(nodes, jobenv)
        self._drop_failed(failed, nodes)
        unavailable.update(failed)

        # mark the set of nodes the resource manager thinks is down
        failed = self.resmgr_down(nodes, jobenv)
        self._drop_failed(failed, nodes)
        unavailable.update(failed)

        # iterate through user selected tests
        for test in self.tests:
            try:
                testmethod = getattr(self, test)
                if callable(testmethod):
                    failed = testmethod(nodes=nodes, jobenv=jobenv)
                    self._drop_failed(failed, nodes)
                    unavailable.update(failed)
                else:
                    print('Nodetests: ERROR: ' + test +
                          ' is defined but is not a test method.')
            except AttributeError as e:
                print('Nodetests: ERROR: ' + test + ' is not defined.')
                print('dir(self)=' + str(dir(self)))
            except Exception as e:
                print('Nodetests: ERROR: Unable to perform the ' + test +
                      ' test.')
                print(e)

        # allow alternate behavior on subsequent runs
        self.firstrun = False

        return unavailable

    def user_excluded(self, nodes=[], jobenv=None):
        # mark any nodes to explicitly exclude via SCR_EXCLUDE_NODES
        failed = {}
        exclude_nodes = jobenv.param.get('SCR_EXCLUDE_NODES')
        nodelist = jobenv.resmgr.expand_hosts(exclude_nodes)
        for node in nodelist:
            failed[node] = 'User excluded via SCR_EXCLUDE_NODES'
        return failed

    def resmgr_down(self, nodes=[], jobenv=None):
        # mark the set of nodes the resource manager thinks is down
        failed = {}
        nodelist = jobenv.resmgr.down_nodes()
        for node in nodelist:
            failed[node] = nodelist[node]
        return failed

    # mark any nodes that fail to respond to (up to 2) ping(s)
    def ping(self, nodes=[], jobenv=None):
        """Attempt to ping each node."""
        failed = {}
        for node in nodes:
            # ping -c 1 -w 1 <host>
            argv = [self.pingexe, '-c', '1', '-w', '1', node]
            returncode = runproc(argv=argv)[1]
            if returncode != 0:
                # ping failed, try one more time just to be sure
                returncode = runproc(argv=argv)[1]
                if returncode != 0:
                    # ping failed twice, consider it down
                    failed[node] = 'Failed to ping'
        return failed

    # mark any nodes that don't respond to pdsh echo up
    def pdsh_echo(self, nodes=[], jobenv=None):
        # assume all nodes are down
        pdsh_assumed_down = nodes.copy()

        # run an "echo UP" on each node to check whether it works
        upnodes = jobenv.resmgr.compress_hosts(nodes)
        output = jobenv.launcher.parallel_exec(argv=['echo', 'UP'],
                                               runnodes=upnodes)[0][0]

        # drop nodes from assumed down list if they responded with 'UP' message
        for line in output.split('\n'):
            if len(line) == 0:
                continue

            if 'UP' in line:
                uphost = line.split(':')[0]
                if uphost in pdsh_assumed_down:
                    pdsh_assumed_down.remove(uphost)

        # check for any nodes still assumed down
        # this will be the set that failed to print an 'UP' messagee
        failed = {}
        for node in pdsh_assumed_down:
            failed[node] = 'Failed to pdsh echo UP'
        return failed

    # mark nodes that fail the capacity check
    def dir_capacity(self, nodes=[], jobenv=None):
        # prepare values to check control directory
        cntl_vals = []
        cntl_dirs = jobenv.dir_control(base=True)
        cntl_sizes = jobenv.param.get_hash('CNTLDIR')
        if cntl_sizes is not None:
            for val in cntl_dirs:
                if val in cntl_sizes and 'BYTES' in cntl_sizes[val]:
                    size = list(cntl_sizes[base]['BYTES'].keys())[0]
                    size = jobenv.param.abtoull(size)
                    val += ':' + str(size)
                cntl_vals.append(val)

        # prepare values to check cache directories
        cache_vals = []
        cache_dirs = jobenv.dir_cache(base=True)
        cache_sizes = jobenv.param.get_hash('CACHEDIR')
        if cache_sizes is not None:
            for val in cache_dirs:
                if val in cache_sizes and 'BYTES' in cache_sizes[val]:
                    size = list(cache_sizes[base]['BYTES'].keys())[0]
                    size = jobenv.param.abtoull(size)
                    val += ':' + str(size)
                cache_vals.append(val)

        # run scr_check_node on each node specifying control and cache directories to check
        check_exe = os.path.join(config.X_LIBEXECDIR, 'python',
                                 'scr_check_node.py')
        argv = [check_exe]
        if self.firstrun:
            argv.append('--free')
        if cntl_vals:
            argv.extend(['--cntl', ','.join(cntl_vals)])
        if cache_vals:
            argv.extend(['--cache', ','.join(cache_vals)])

        # only run this against set of nodes known to be responding
        upnodes = jobenv.resmgr.compress_hosts(nodes)
        output = jobenv.launcher.parallel_exec(argv=argv,
                                               runnodes=upnodes)[0][0]

        # drop any nodes that report FAIL
        failed = {}
        for line in output.split('\n'):
            if line == '':
                continue

            if 'FAIL' in line:
                parts = line.split(':')
                node = parts[0]
                failed[node] = parts[1][1:]

        return failed
