import os
import re
import datetime

from scrjob import hostlist
from scrjob.common import runproc
from scrjob.resmgrs import ResourceManager


class SLURM(ResourceManager):

    def __init__(self):
        super(SLURM, self).__init__(resmgr='SLURM')

    def prerun(self):
        # HACK: NOP srun to force every node to run SLURM prolog to delete files from cache
        # remove this if admins find a better place to clear cache
        argv = ['srun', '/bin/hostname']  # ,'>','/dev/null']
        runproc(argv=argv)

    # get SLURM jobid of current allocation
    def job_id(self):
        return os.environ.get('SLURM_JOBID')

    # get node list
    def job_nodes(self):
        nodelist = os.environ.get('SLURM_NODELIST')
        return hostlist.expand_hosts(nodelist)

    # use sinfo to query SLURM for the list of nodes it thinks to be down
    def down_nodes(self):
        downnodes = {}
        nodelist = self.job_nodes()
        if nodelist:
            nodestr = hostlist.join_hosts(nodelist)
            down, returncode = runproc("sinfo -ho %N -t down -n " + nodestr,
                                       getstdout=True)
            if returncode == 0:
                #### verify this format, comma separated list
                ### if nodes may be duplicated convert list to set then to list again
                down = down.strip()
                nodelist = list(set(down.split(',')))
                for node in nodelist:
                    if node != '':
                        downnodes[node] = 'Reported down by resource manager'
        return downnodes

    # query SLURM for allocation endtime, expressed as secs since epoch
    def end_time(self):
        # get jobid
        jobid = self.job_id()
        if jobid is None:
            return 0

        # TODO: we can probably get this from a library to avoid fork/exec/parsing
        # ask scontrol for endtime of this job
        output = runproc("scontrol --oneliner show job " + jobid,
                         getstdout=True)[0]
        m = re.search('EndTime=(\\S*)', output)
        if not m:
            # failed to parse output, return 0 to indicate unknown
            return 0

        # 'Unknown' is returned when no time limit was set
        timestr = m.group(1)
        if timestr == 'Unknown':
            return 0

        # parse time string like "2021-07-16T14:05:12" into secs since epoch
        dt = datetime.datetime.strptime(timestr, "%Y-%m-%dT%H:%M:%S")
        timestamp = int(dt.strftime("%s"))
        return timestamp
