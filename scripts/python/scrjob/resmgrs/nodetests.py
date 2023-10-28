import os

from scrjob import config
from scrjob.common import runproc


class Nodetests:

    def __init__(self):
        # other tests could perform alternate behavior during first/subsequent runs
        self.firstrun = True
        # the ping executable
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
                with open(config.SCR_NODE_TESTS_FILE, 'r') as infile:
                    for line in infile.readlines():
                        line = line.strip()
                        if line != '':
                            self.tests.extend(line.split(','))
            except:
                print('nodetests.py: ERROR: Unable to open file ' +
                      config.SCR_NODE_TESTS_FILE)
        else:
            self.tests = []

    def __call__(self, nodes=[], jobenv=None):
        if type(nodes) is str:
            nodes = nodes.split(',')
        # This method returns a dictionary of unavailable nodes
        #   keyed on node with the reason as the value
        unavailable = {}
        # mark any nodes to explicitly exclude via SCR_EXCLUDE_NODES
        nodelist = jobenv.resmgr.expand_hosts(
            jobenv.param.get('SCR_EXCLUDE_NODES'))
        for node in nodelist:
            if node == '':
                continue
            if node in nodes:
                nodes.remove(node)
            unavailable[node] = 'User excluded via SCR_EXCLUDE_NODES'
        # mark the set of nodes the resource manager thinks is down
        nodelist = jobenv.resmgr.down_nodes()
        for node in nodelist:
            if node == '':
                continue
            if node in nodes:
                nodes.remove(node)
            unavailable[node] = nodelist[node]
        # iterate through user selected tests
        for test in self.tests:
            # if all of the nodes have failed discontinue the tests
            if nodes == []:
                break
            try:
                testmethod = getattr(self, test)
                if callable(testmethod):
                    nextunavailable = testmethod(nodes=nodes, jobenv=jobenv)
                    unavailable.update(nextunavailable)
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

    # mark any nodes that fail to respond to (up to 2) ping(s)
    def ping(self, nodes=[], jobenv=None):
        unavailable = {}
        # `$ping -c 1 -w 1 $node 2>&1 || $ping -c 1 -w 1 $node 2>&1`;
        argv = [self.pingexe, '-c', '1', '-w', '1', '']
        for node in nodes:
            argv[5] = node
            returncode = runproc(argv=argv)[1]
            if returncode != 0:
                returncode = runproc(argv=argv)[1]
                if returncode != 0:
                    unavailable[node] = 'Failed to ping'
        for node in unavailable:
            if node in nodes:
                nodes.remove(node)
        return unavailable

    # mark any nodes that don't respond to pdsh echo up
    def pdsh_echo(self, nodes=[], jobenv=None):
        unavailable = {}
        pdsh_assumed_down = nodes.copy()
        # only run this against set of nodes known to be responding
        # run an "echo UP" on each node to check whether it works
        output = jobenv.launcher.parallel_exec(argv=['echo', 'UP'],
                                               runnodes=','.join(nodes))[0][0]
        for line in output.split('\n'):
            if len(line) == 0:
                continue
            if 'UP' in line:
                uphost = line.split(':')[0]
                if uphost in pdsh_assumed_down:
                    pdsh_assumed_down.remove(uphost)

        # if we still have any nodes assumed down, update our available/unavailable lists
        for node in pdsh_assumed_down:
            nodes.remove(node)
            unavailable[node] = 'Failed to pdsh echo UP'
        return unavailable

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
        unavailable = {}
        for line in output.split('\n'):
            if line == '':
                continue
            if 'FAIL' in line:
                parts = line.split(':')
                node = parts[0]
                if node in nodes:
                    nodes.remove(node)
                    unavailable[node] = parts[1][1:]
        return unavailable
