from scrjob.nodetests import NodeTest


class Echo(NodeTest):
    """Attempt to "echo UP" on each node."""

    def __init__(self):
        pass

    # mark any nodes that fail to respond to (up to 2) ping(s)
    def execute(self, nodes, jobenv):
        # run an "echo UP" on each node to check whether it works
        argv = ['echo', 'UP']
        result = jobenv.rexec.rexec(argv, nodes, jobenv)

        # verify that we find UP in stdout from each node
        failed = {}
        for node in nodes:
            if 'UP' not in result.stdout(node):
                failed[node] = 'Failed to echo UP'
        return failed
