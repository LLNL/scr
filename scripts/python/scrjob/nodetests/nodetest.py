class NodeTest(object):

    def __init__(self):
        pass

    def execute(self, nodes, jobenv):
        """Returns a dictionary of failed nodes.

        Executes the test over the provided list of nodes.
        For any node that fails the test, an entry is added to the dictionary.
        The key is the node name.
        The value provides a short reason indicating which test failed:
            failed = {}
            failed[node] = 'Failed test such and such.'
            return failed
        """
        pass
