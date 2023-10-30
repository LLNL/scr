import os
import re
from time import time

# flux imports
try:
    import flux
    from flux.job import JobspecV1
    from flux.resource import ResourceSet
    from flux.rpc import RPC
except:
    pass

from scrjob.remoteexec import RemoteExec


class FLUX(RemoteExec):

    def __init__(self):
        # connect to the running Flux instance
        try:
            self.flux = flux.Flux()
        except:
            raise ImportError(
                'Error importing flux, ensure that the flux daemon is running.'
            )

    def rexec(self, argv, nodes, jobenv):
        nnodes, ntasks, ncores, argv = self.parsefluxargs(argv)

        ### Need to determine number of nodes to set nnodes and nntasks to N
        ### without specifying it is set above to just launch 1 task on 1 cpu on 1 node
        ### The size is the number of ranks flux start was launched with
        resp = RPC(self.flux, 'resource.status').get()
        rset = ResourceSet(resp['R'])
        nnodes = rset.nnodes
        ntasks = nnodes
        compute_jobreq = JobspecV1.from_command(command=argv,
                                                num_tasks=ntasks,
                                                num_nodes=nnodes,
                                                cores_per_task=ncores)

        ### create a yaml 'file' stream from a string to get JobspecV1.from_yaml_stream()
        #   This may allow to explicitly specify the nodes to run on
        # string = 'yaml spec'
        # stream = io.StringIO(string)
        # compute_jobreq = JobspecV1.from_yaml_stream(stream)
        compute_jobreq.cwd = os.getcwd()
        compute_jobreq.environment = dict(os.environ)
        compute_jobreq.setattr_shell_option("output.stdout.label", True)
        compute_jobreq.setattr_shell_option("output.stderr.label", True)
        prefix = jobenv.dir_scr()
        timestamp = str(time())

        # time will return posix timestamp like -> '1628179160.1724932'
        # some unique filename to send stdout/stderr to
        ### the script prepends the rank to stdout, not stderr.
        outfilename = 'out' + timestamp
        errfilename = 'err' + timestamp
        outfilename = os.path.join(prefix, outfilename)
        errfilename = os.path.join(prefix, errfilename)

        # all tasks will write their stdout to this file
        compute_jobreq.stdout = outfilename
        compute_jobreq.stderr = errfilename
        job = flux.job.submit(self.flux, compute_jobreq, waitable=True)

        # get the hostlist to swap ranks for hosts
        nodelist = rset.nodelist
        future = flux.job.wait_async(self.flux, job)
        status = future.get_status()
        ret = [['', ''], 0]

        # don't fail if can't open a file, just leave output blank
        try:
            with open(outfilename, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    try:
                        rank = re.search('\d', line)
                        host = nodelist[int(rank[0])]
                        line = host + line[line.find(':'):]
                    except:
                        pass
                    ret[0][0] += line
            os.remove(outfilename)
        except:
            pass

        try:
            with open(errfilename, 'r') as f:
                lines = f.readlines()
                for line in lines:
                    try:
                        rank = re.search('\d', line)
                        host = nodelist[int(rank[0])]
                        line = host + line[line.find(':'):]
                    except:
                        pass
                    ret[0][1] += line
        except:
            try:
                ret[0][1] = status.errstr.decode('UTF-8')
            except:
                pass

        # stderr set in a nested try, remove the errfile here
        try:
            os.remove(errfilename)
        except:
            pass

        ret[1] = 0 if status.success == True else 1
        return ret
