import os

from scrjob import config
from scrjob.nodetests import NodeTest


class DirCapacity(NodeTest):
    """Run scr_check_node.py on each node.

    Checks that cache and control dirs are writable and optionally have
    a given minimum capacity.
    """

    def __init__(self):
        pass

    def execute(self, nodes, jobenv):
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
        argv = ['python3', check_exe]
        #        if self.firstrun:
        #            argv.append('--free')
        if cntl_vals:
            argv.extend(['--cntl', ','.join(cntl_vals)])
        if cache_vals:
            argv.extend(['--cache', ','.join(cache_vals)])

        result = jobenv.rexec.rexec(argv, nodes, jobenv)

        # drop any nodes that do not report PASS
        failed = {}
        for node in nodes:
            if 'PASS' not in result.stdout(node):
                failed[node] = result.stdout(node)
        return failed
