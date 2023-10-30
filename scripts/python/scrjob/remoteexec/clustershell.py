from scrjob import config
from scrjob.remoteexec import RemoteExec


class ClusterShell(RemoteExec):

    def __init__(self):
        self.Task = None
        try:
            import ClusterShell.Task as Task
            self.Task = Task
        except:
            raise RuntimeError('Failed to import ClusterShell')

    #####
    # https://clustershell.readthedocs.io/en/latest/api/Task.html
    # clustershell exec can be called from any sub-resource manager
    # the sub-resource manager is responsible for ensuring clustershell is available
    ### TODO: different ssh programs may need different parameters added to remove the 'tput: ' from the output
    def rexec(self, argv, nodes, jobenv):
        argv = ' '.join(argv)
        nodes = self.join_hosts(nodes)

        # launch the task
        task = self.Task.task_self()
        task.run(argv, nodes=nodes)

        # ensure all of the tasks have completed
        self.Task.task_wait()

        # iterate through the task.iter_retcodes() to get (return code, [nodes])
        # to get msg objects, output must be retrieved by individual node using task.node_buffer or .key_error
        # retrieved outputs are bytes, convert with .decode('utf-8')
        ret = [['', ''], 0]
        for rc, keys in task.iter_retcodes():
            if rc != 0:
                ret[1] = 1

            for host in keys:
                output = task.node_buffer(host).decode('utf-8')
                for line in output.split('\n'):
                    if line != '' and line != 'tput: No value for $TERM and no -T specified':
                        ret[0][0] += host + ': ' + line + '\n'

                output = task.key_error(host).decode('utf-8')
                for line in output.split('\n'):
                    if line != '' and line != 'tput: No value for $TERM and no -T specified':
                        ret[0][1] += host + ': ' + line + '\n'
        return ret
