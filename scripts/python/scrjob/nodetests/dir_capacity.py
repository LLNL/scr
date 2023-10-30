import os

from scrjob import config
from scrjob.nodetests import NodeTest


class DirCapacity(NodeTest):
    """Run scr_check_node.py on each node"""

    def __init__(self, jobenv):
        super(DirCapacity, self).__init__(jobenv)
        self.check_exe = os.path.join(config.X_LIBEXECDIR, 'python', 'scr_check_node.py')

    def execute(self, nodes):
        # prepare values to check control directory
        cntl_vals = []
        cntl_dirs = self.jobenv.dir_control(base=True)
        cntl_sizes = self.jobenv.param.get_hash('CNTLDIR')
        if cntl_sizes is not None:
            for val in cntl_dirs:
                if val in cntl_sizes and 'BYTES' in cntl_sizes[val]:
                    size = list(cntl_sizes[base]['BYTES'].keys())[0]
                    size = self.jobenv.param.abtoull(size)
                    val += ':' + str(size)
                cntl_vals.append(val)

        # prepare values to check cache directories
        cache_vals = []
        cache_dirs = self.jobenv.dir_cache(base=True)
        cache_sizes = self.jobenv.param.get_hash('CACHEDIR')
        if cache_sizes is not None:
            for val in cache_dirs:
                if val in cache_sizes and 'BYTES' in cache_sizes[val]:
                    size = list(cache_sizes[base]['BYTES'].keys())[0]
                    size = self.jobenv.param.abtoull(size)
                    val += ':' + str(size)
                cache_vals.append(val)

        # run scr_check_node on each node specifying control and cache directories to check
        argv = [self.check_exe]
        if self.firstrun:
            argv.append('--free')
        if cntl_vals:
            argv.extend(['--cntl', ','.join(cntl_vals)])
        if cache_vals:
            argv.extend(['--cache', ','.join(cache_vals)])

        output = self.pexec(argv, nodes)[0][0]

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
