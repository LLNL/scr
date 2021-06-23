#! /usr/bin/env python

# scr_env_pmix.py
# SCR_Env_PMIX is a subclass if SCR_Env_Base

import os
from pyfe import scr_const, scr_hostlist
from pyfe.env.scr_env_base import SCR_Env_Base
from pyfe.scr_common import runproc

class SCR_Env_PMIX(SCR_Env_Base):
  # init initializes vars from the environment
  def __init__(self):
    super(SCR_Env_PMIX, self).__init__(env='PMIX')

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

