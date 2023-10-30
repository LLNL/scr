from scrjob.nodetests import NodeTest


class Echo(NodeTest):
    """Attempt to echo "UP" from each node."""

    def __init__(self):
        pass

    # mark any nodes that fail to respond to (up to 2) ping(s)
    def execute(self, nodes, jobenv):
        # assume all nodes are down
        assume_down = nodes.copy()

        # run an "echo UP" on each node to check whether it works
        argv = ['echo', 'UP']
        output = self.parexec(argv, nodes, jobenv)[0][0]

        # drop nodes from assumed down list if they responded with 'UP' message
        for line in output.split('\n'):
            # skip empty lines
            if len(line) == 0:
                continue

            if 'UP' in line:
                node = line.split(':')[0]
                if node in assume_down:
                    assume_down.remove(node)

        # check for any nodes still in the assumed down list
        # this is the set that failed to print an 'UP' message
        failed = {}
        for node in assume_down:
            failed[node] = 'Failed to echo UP'
        return failed
