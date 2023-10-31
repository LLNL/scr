from scrjob import config
from scrjob.remoteexec import RemoteExec, RemoteExecResult


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
        result = RemoteExecResult(argv, nodes)

        task = self.Task.task_self()

        # launch the task
        argv = ' '.join(argv)
        nodes = ','.join(nodes)
        task.run(argv, nodes=nodes)

        # ensure all of the tasks have completed
        self.Task.task_wait()

        # iterate through the task.iter_retcodes() to get (return code, [nodes])
        # to get msg objects, output must be retrieved by individual node using task.node_buffer or .key_error
        # retrieved outputs are bytes, convert with .decode('utf-8')
        for rc, keys in task.iter_retcodes():
            for host in keys:
                result.set_rc(host, rc)

                output = task.key_buffer(host)
                if output:
                    result.append_stdout(host, output.decode('utf-8'))

                output = task.key_error(host)
                if output:
                    result.append_stderr(host, output.decode('utf-8'))

        return result
