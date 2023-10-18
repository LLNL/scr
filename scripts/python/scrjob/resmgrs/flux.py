import os, re
import datetime
from time import time

from scrjob import scr_const
from scrjob.resmgrs import nodetests, ResourceManager

# flux imports
try:
    import flux
    from flux.hostlist import Hostlist
    from flux.resource import ResourceSet
    from flux.rpc import RPC
    from flux.job import JobID, JobInfo, JobList
except:
    pass


class FLUX(ResourceManager):
    # init initializes vars from the environment
    def __init__(self):
        # the super.init() calls resmgr.job_nodes, we must set self.flux first
        try:
            self.flux = flux.Flux()
        except:
            raise ImportError(
                'Error importing flux, ensure that the flux daemon is running.'
            )
        super(FLUX, self).__init__(resmgr='FLUX')
        self.jobid = self.job_id()

    ####
    # the job id of the allocation is needed in postrun/list_dir
    # the job id is a component of the path.
    # We can either copy methods from existing resource managers . . .
    # or we can use the POSIX timestamp and set the value at __init__
    def job_id(self):
        if self.jobid is not None:
            return self.jobid

        jobid_str = os.environ.get('FLUX_JOB_ID')
        if jobid_str is None:
            jobid = JobID(self.flux.attr_get("jobid"))
        else:
            jobid = self.flux.job.JobID.id_parse(jobid_str)
        return str(jobid)

    # get node list
    def job_nodes(self):
        resp = RPC(self.flux, "resource.status").get()
        rset = ResourceSet(resp["R"])
        return str(rset.nodelist)

    def down_nodes(self):
        downnodes = {}
        resp = RPC(self.flux, "resource.status").get()
        rset = ResourceSet(resp["R"])
        offline = str(resp['offline'])
        exclude = str(resp['exclude'])
        offline = self.expand_hosts(offline)
        exclude = self.expand_hosts(offline)
        for node in offline:
            if node != '' and node not in downnodes:
                downnodes[node] = 'Reported down by resource manager'
        for node in exclude:
            if node != '' and node not in downnodes:
                downnodes[node] = 'Excluded by resource manager'
        return downnodes

    def end_time(self):
        jobid_str = os.environ.get('FLUX_JOB_ID')
        if jobid_str is None:
            parent = flux.Flux(self.flux.attr_get("parent-uri"))
            info = JobList(parent, ids=[self.jobid]).fetch_jobs().get_jobs()[0]
        else:
            info = JobList(self.flux,
                           ids=[self.jobid]).fetch_jobs().get_jobs()[0]
        endtime = info["expiration"]
        return endtime
