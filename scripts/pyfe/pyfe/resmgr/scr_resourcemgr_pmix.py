#! /usr/bin/env python3

# scr_resourcemgr_pmix.py
# SCR_Resourcemgr_PMIX is a subclass if SCR_Resourcemgr_Base

import os
from pyfe import scr_const, scr_hostlist
from pyfe.resmgr.scr_resourcemgr_base import SCR_Resourcemgr_Base
from pyfe.scr_common import runproc

class SCR_Resourcemgr_PMIX(SCR_Resourcemgr_Base):
  # init initializes vars from the environment
  def __init__(self):
    super(SCR_Resourcemgr_PMIX, self).__init__(resmgr='PMIX')

  # get job id, setting environment flag here
  def getjobid(self):
    # CALL SCR_ENV_HELPER FOR PMIX
    # failed to read jobid from environment,
    # assume user is running in test mode
    return 'defjobid'

  # get node list
  def get_job_nodes(self):
    val = os.environ.get('PMIX_NODELIST')
    if val is not None:
      node_list = val.split(',')
      nodeset = scr_hostlist.compress(node_list)
      return nodeset
    return None

  def get_downnodes(self):
    # if the resource manager knows any nodes to be down out of the job's
    # nodeset, print this list in 'atlas[30-33,35,45-53]' form
    # if there are none, print nothing, not even a newline
    # CALL SCR_ENV_HELPER FOR PMIX - THIS IS A TODO AS PMIX DOESN'T SUPPORT IT YET
    #if (0) {
    #  my $nodeset = ""; #get nodeset with pmixhelper
    return None

  # set down node list, requires node list to already be set
  def set_downnodes(self):
    self.conf['down'] = '' # TODO # parse out

  # list the number of nodes used in the last run
  def get_runnode_count(self):
    argv = [self.conf['nodes_file'],'--dir',self.conf['prefix']]
    out, returncode = runproc(argv=argv,getstdout=True)
    if returncode==0:
      return int(out)
    return 0 # print(err)

  # TODO: cppr/pmix notes - this script is only used when launching the watchdog process.  Have not tested this
  def get_jobstep_id(user='',jobid='',pid=-2):
    # get job steps for this user and job, order by decreasing job step
    # so first one should be the one we are looking for
    # -h means print no header, so just the data in this order:
    # STEPID         NAME PARTITION     USER      TIME NODELIST

    #argv = []
    #output = runproc(argv=argv,getstdout=True)[0].strip()
    #output = output.split('\n')

    currjobid=-1
    return currjobid
