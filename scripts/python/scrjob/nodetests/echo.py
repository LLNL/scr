from scrjob.nodetests import NodeTest


class Echo(NodeTest):
    """Attempt to echo "UP" from each node."""

    def __init__(self):
        pass

    # mark any nodes that fail to respond to (up to 2) ping(s)
    def execute(self, nodes, jobenv):
        # run an "echo UP" on each node to check whether it works
        argv = ['echo', 'UP']
        result = jobenv.rexec.rexec(argv, nodes, jobenv)

        # check for any nodes still in the assumed down list
        # this is the set that failed to print an 'UP' message
        failed = {}
        for node in nodes:
            if 'UP' not in result.stdout(node):
                failed[node] = 'Failed to echo UP'
        return failed
