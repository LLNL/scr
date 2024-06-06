"""Defines the Flux ResourceManager subclass."""

import os

from scrjob.resmgrs import ResourceManager

# flux imports
try:
    import flux
    import flux.resource
    from flux.job import JobID, JobList
except ImportError:
    pass


class FLUX(ResourceManager):
    """Represents the Flux resource manager."""

    def __init__(self):
        """Initializes vars from the environment.

        The super.init() calls resmgr.job_nodes, we must set self.flux first.
        """
        try:
            self.flux = flux.Flux()
        except NameError as exc:
            raise ImportError(
                'Error importing flux, ensure that the flux daemon is running.'
            ) from exc
        super().__init__(resmgr='FLUX')
        self.jobid = None  # set it so that self.job_id() doesn't error out
        self.jobid = self.job_id()

    def job_id(self):
        """Fetch the job ID.

        The job id of the allocation is needed in postrun/list_dir
        the job id is a component of the path.
        """
        if self.jobid is not None:
            return self.jobid

        jobid_str = os.environ.get('FLUX_JOB_ID')
        if jobid_str is None:
            jobid = JobID(self.flux.attr_get("jobid"))
        else:
            jobid = self.flux.job.JobID.id_parse(jobid_str)
        return str(jobid)

    def job_nodes(self):
        """Get node list."""
        return list(flux.resource.resource_list(self.flux).get().all.nodelist)

    def down_nodes(self):
        """Return list of down nodes."""
        return {
            nodename: "Reported down by resource manager"
            for nodename in flux.resource.resource_list(self.flux).get().down.nodelist
        }

    def end_time(self):
        """Get the end time of the current allocation."""
        jobid_str = os.environ.get('FLUX_JOB_ID')
        if jobid_str is None:
            parent = flux.Flux(self.flux.attr_get("parent-uri"))
            info = JobList(parent, ids=[self.jobid]).fetch_jobs().get_jobs()[0]
        else:
            info = JobList(self.flux,
                           ids=[self.jobid]).fetch_jobs().get_jobs()[0]
        endtime = info["expiration"]
        return endtime
