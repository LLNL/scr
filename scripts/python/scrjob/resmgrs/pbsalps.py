import os

from scrjob.common import runproc, pipeproc
from scrjob.resmgrs import ResourceManager


class PBSALPS(ResourceManager):
    # init initializes vars from the environment
    def __init__(self, env=None):
        super(PBSALPS, self).__init__(resmgr='PBSALPS')

    # get job id, setting environment flag here
    def job_id(self):
        # val may be None
        return os.environ.get('PBS_JOBID')

    # get node list
    def job_nodes(self):
        val = os.environ.get('PBS_NUM_NODES')
        if val:
            cmd = "aprun -n " + val + " -N 1 cat /proc/cray_xt/nid"  # $nidfile
            out = runproc(cmd, getstdout=True)[0]
            nodearray = out.split('\n')
            if len(nodearray) > 0:
                if nodearray[-1] == '\n':
                    nodearray = nodearray[:-1]
                if len(nodearray) > 0:
                    if nodearray[-1].startswith('Application'):
                        nodearray = nodearray[:-1]
                    return nodearray
        return []

    def down_nodes(self):
        downnodes = {}
        snodes = self.job_nodes()
        for node in snodes:
            out, returncode = runproc("xtprocadmin -n " + node, getstdout=True)
            #if returncode==0:
            resarray = out.split('\n')
            answerarray = resarray[1].split(' ')
            answer = answerarray[4]
            if 'down' in answer:
                downnodes[node] = 'Reported down by resource manager'
        return downnodes
