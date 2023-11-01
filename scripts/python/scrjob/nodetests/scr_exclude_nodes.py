from scrjob import hostlist
from scrjob.nodetests import NodeTest


class SCRExcludeNodes(NodeTest):
    """Exclude nodes listed in SCR_EXCLUDE_NODES."""

    def __init__(self):
        pass

    def execute(self, nodes, jobenv):
        failed = {}
        exclude_nodes = jobenv.param.get('SCR_EXCLUDE_NODES')
        nodelist = hostlist.expand_hosts(exclude_nodes)
        for node in nodelist:
            if node in nodes:
                failed[node] = 'User excluded via SCR_EXCLUDE_NODES'
        return failed
