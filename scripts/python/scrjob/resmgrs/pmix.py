"""# pmix.py # PMIX is a subclass if ResourceManager

import os

from scrjob import config, hostlist
from scrjob.common import runproc, pipeproc
from scrjob.resmgrs import ResourceManager

class PMIX(ResourceManager):
  # init initializes vars from the environment
  def __init__(self):
    super(PMIX, self).__init__(resmgr='PMIX')

  # get job id, setting environment flag here
  def job_id(self):
    if self.jobid is not None:
      return self.jobid
    #####
    # CALL SCR_ENV_HELPER FOR PMIX
    # failed to read jobid from environment,
    # assume user is running in test mode
    return None

  # TODO: cppr/pmix notes - this script is only used when launching the watchdog process.  Have not tested this
  def jobstep_id(self,user='',pid=-1):
    # get job steps for this user and job, order by decreasing job step
    # so first one should be the one we are looking for
    # -h means print no header, so just the data in this order:
    # STEPID         NAME PARTITION     USER      TIME NODELIST

    #argv = []
    #output = runproc(argv=argv,getstdout=True)[0].strip()
    #output = output.split('\n')

    currjobid=None
    return currjobid

  # get node list
  def job_nodes(self):
    nodelist = os.environ.get('PMIX_NODELIST')
    return hostlist.expand_hosts(nodelist)

  def down_nodes(self):
    # if the resource manager knows any nodes to be down out of the job's
    # nodeset, print this list in 'atlas[30-33,35,45-53]' form
    # if there are none, print nothing, not even a newline
    # CALL SCR_ENV_HELPER FOR PMIX - THIS IS A TODO AS PMIX DOESN'T SUPPORT IT YET
    #if (0) {
    #  my $nodeset = ""; #get nodeset with pmixhelper
    return None

  def kill_jobstep(self,jobid=-1):
    print('pmix does not support this')
    return 1
"""
