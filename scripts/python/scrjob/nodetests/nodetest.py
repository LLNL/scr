class NodeTest(object):

    def __init__(self, jobenv):
        self.jobenv = jobenv

    def parexec(self, argv, nodes):
        nodes = self.jobenv.resmgr.compress_hosts(nodes)
        return self.jobenv.launcher.parallel_exec(argv=argv, runnodes=nodes)

    def execute(self, nodes):
        pass
