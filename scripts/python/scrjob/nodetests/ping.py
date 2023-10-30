from scrjob.nodetests import NodeTest
from scrjob.common import runproc


class Ping(NodeTest):
    """Attempt to ping each node."""

    def __init__(self):
        # path to the ping executable
        self.ping_exe = 'ping'

    # mark any nodes that fail to respond to (up to 2) ping(s)
    def execute(self, nodes, jobenv):
        failed = {}

        for node in nodes:
            # ping -c 1 -w 1 <host>
            argv = [self.ping_exe, '-c', '1', '-w', '1', node]
            rc = runproc(argv=argv)[1]
            if rc != 0:
                # ping failed, try one more time just to be sure
                rc = runproc(argv=argv)[1]
                if rc != 0:
                    # ping failed twice, consider it down
                    failed[node] = 'Failed to ping'

        return failed
