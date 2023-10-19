import os
from scrjob import scr_const
from scrjob.scr_common import runproc
from scrjob.list_dir import list_dir


class Nodetests:

    def __init__(self):
        # The list_dir() method uses a free flag iff it is the first run
        # other tests could perform alternate behavior during first/subsequent runs
        self.firstrun = True
        # the ping executable
        self.pingexe = 'ping'
        # First try to read the environment variable
        self.tests = os.environ.get('SCR_NODE_TESTS')
        if self.tests is not None:
            self.tests = self.tests.split(',')
        elif scr_const.SCR_NODE_TESTS != '':
            self.tests = scr_const.SCR_NODE_TESTS.split(',')
        elif scr_const.SCR_NODE_TESTS_FILE != '':
            try:
                self.tests = []
                with open(scr_const.SCR_NODE_TESTS_FILE, 'r') as infile:
                    for line in infile.readlines():
                        line = line.strip()
                        if line != '':
                            self.tests.extend(line.split(','))
            except:
                print('nodetests.py: ERROR: Unable to open file ' +
                      scr_const.SCR_NODE_TESTS_FILE)
        else:
            self.tests = []

    def __call__(self, nodes=[], scr_env=None):
        if type(nodes) is str:
            nodes = nodes.split(',')
        # This method returns a dictionary of unavailable nodes
        #   keyed on node with the reason as the value
        unavailable = {}
        # mark any nodes to explicitly exclude via SCR_EXCLUDE_NODES
        nodelist = scr_env.resmgr.expand_hosts(
            scr_env.param.get('SCR_EXCLUDE_NODES'))
        for node in nodelist:
            if node == '':
                continue
            if node in nodes:
                nodes.remove(node)
            unavailable[node] = 'User excluded via SCR_EXCLUDE_NODES'
        # mark the set of nodes the resource manager thinks is down
        nodelist = scr_env.resmgr.down_nodes()
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
                    nextunavailable = testmethod(nodes=nodes, scr_env=scr_env)
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
    def ping(self, nodes=[], scr_env=None):
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
    def pdsh_echo(self, nodes=[], scr_env=None):
        unavailable = {}
        pdsh_assumed_down = nodes.copy()
        # only run this against set of nodes known to be responding
        # run an "echo UP" on each node to check whether it works
        output = scr_env.launcher.parallel_exec(argv=['echo', 'UP'],
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
    def dir_capacity(
            self,
            nodes=[],
            #free=False, ###free is only true during the _first_ run
            scr_env=None):
        cntldir_string = list_dir(base=True,
                                  runcmd='control',
                                  scr_env=scr_env,
                                  bindir=scr_const.X_BINDIR)
        cachedir_string = list_dir(base=True,
                                   runcmd='cache',
                                   scr_env=scr_env,
                                   bindir=scr_const.X_BINDIR)
        unavailable = {}
        param = scr_env.param
        # specify whether to check total or free capacity in directories
        #if free: free_flag = '--free'

        # check that control and cache directories on each node work and are of proper size
        # get the control directory the job will use
        cntldir_vals = []
        # cntldir_string = `$bindir/scr_list_dir --base control`;
        if type(cntldir_string) is str and len(cntldir_string) != 0:
            dirs = cntldir_string.split(' ')
            cntldirs = param.get_hash('CNTLDIR')
            for base in dirs:
                if len(base) < 1:
                    continue
                val = base
                if cntldirs is not None and base in cntldirs and 'BYTES' in cntldirs[
                        base]:
                    if len(cntldirs[base]['BYTES'].keys()) > 0:
                        size = list(cntldirs[base]['BYTES'].keys())[
                            0]  #(keys %{$$cntldirs{$base}{"BYTES"}})[0];
                        #if (defined $size) {
                        size = param.abtoull(size)
                        #  $size = $param->abtoull($size);
                        val += ':' + str(size)
                        #  $val = "$base:$size";
                cntldir_vals.append(val)

        cntldir_flag = []
        if len(cntldir_vals) > 0:
            cntldir_flag = ['--cntl', ','.join(cntldir_vals)]

        # get the cache directory the job will use
        cachedir_vals = []
        #`$bindir/scr_list_dir --base cache`;
        if type(cachedir_string) is str and len(cachedir_string) != 0:
            dirs = cachedir_string.split(' ')
            cachedirs = param.get_hash('CACHEDIR')
            for base in dirs:
                if len(base) < 1:
                    continue
                val = base
                if cachedirs is not None and base in cachedirs and 'BYTES' in cachedirs[
                        base]:
                    if len(cachedirs[base]['BYTES'].keys()) > 0:
                        size = list(cachedirs[base]['BYTES'].keys())[0]
                        #my $size = (keys %{$$cachedirs{$base}{"BYTES"}})[0];
                        #if (defined $size) {
                        size = param.abtoull(size)
                        #  $size = $param->abtoull($size);
                        val += ':' + str(size)
                        #  $val = "$base:$size";
                cachedir_vals.append(val)

        cachedir_flag = []
        if len(cachedir_vals) > 0:
            cachedir_flag = ['--cache', ','.join(cachedir_vals)]

        # only run this against set of nodes known to be responding
        upnodes = scr_env.resmgr.compress_hosts(nodes)

        # run scr_check_node on each node specifying control and cache directories to check
        argv = [scr_const.X_BINDIR + '/scrpy/scrjob/scr_check_node.py']
        if self.firstrun:
            argv.append('--free')
        argv.extend(cntldir_flag)
        argv.extend(cachedir_flag)
        output = scr_env.launcher.parallel_exec(argv=argv,
                                                runnodes=upnodes)[0][0]
        action = 0  # tracking action to use range iterator and follow original line <- shift flow
        nodeset = ''
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
