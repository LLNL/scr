from scrjob.nodetests import NodeTest


class ResMgrDown(NodeTest):
    """Nodes that resource manager reports to be down."""

    def __init__(self):
        pass

    def execute(self, nodes, jobenv):
        failed = {}
        nodelist = jobenv.resmgr.down_nodes()
        for node in nodelist:
            if node in nodes:
                failed[node] = nodelist[node]
        return failed
