#! /usr/bin/env python3

# pmix.py
# PMIX is a subclass if ResourceManager

import os
from pyfe import scr_const, scr_hostlist
from pyfe.scr_common import runproc
from pyfe.resmgr import nodetests, ResourceManager

class PMIX(ResourceManager):
  # init initializes vars from the environment
  def __init__(self):
    super(PMIX, self).__init__(resmgr='PMIX')

  # get job id, setting environment flag here
  def getjobid(self):
    # CALL SCR_ENV_HELPER FOR PMIX
    # failed to read jobid from environment,
    # assume user is running in test mode
    return None

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

  # TODO: cppr/pmix notes - this script is only used when launching the watchdog process.  Have not tested this
  def get_jobstep_id(self,user='',pid=-1):
    # get job steps for this user and job, order by decreasing job step
    # so first one should be the one we are looking for
    # -h means print no header, so just the data in this order:
    # STEPID         NAME PARTITION     USER      TIME NODELIST

    #argv = []
    #output = runproc(argv=argv,getstdout=True)[0].strip()
    #output = output.split('\n')

    currjobid=-1
    return currjobid

  def scr_kill_jobstep(self,jobid=-1):
    print('pmix does not support this')
    return 1

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[],scr_env=None,free=False):
    unavailable = nodetests.list_resmgr_down_nodes(nodes=nodes,resmgr_nodes=self.get_downnodes())
    nextunavail = nodetests.list_nodes_failed_ping(nodes=nodes)
    unavailable.update(nextunavail)
    if scr_env is not None:
      nextunavail = nodetests.list_param_excluded_nodes(nodes=nodes,param=scr_env.param)
      unavailable.update(nextunavail)
      argv = [ '$pdsh','-f','256','-w','$upnodes' ]
      #my $output = `$pdsh -f 256 -w '$upnodes' "$bindir/scr_check_node $free_flag $cntldir_flag $cachedir_flag" | $dshbak -c`;
      nextunavail = nodetests.check_dir_capacity(nodes=nodes, free=free, scr_env=scr_env, scr_check_node_argv=argv)
      unavailable.update(nextunavail)
    return unavailable

  def get_scavenge_pdsh_cmd(self):
    argv = ['$pdsh', '-f', '256', '-S', '-w', '$upnodes']
    cppr_lib = scr_const.CPPR_LDFLAGS
    if cppr_lib.startswith('-L'):
      cppr_lib = cppr_lib[2:]
      argv.append('LD_LIBRARY_PATH=$LD_LIBRARY_PATH:'+cpprlib)
    cppr_prefix = os.environ.get('CPPR_PREFIX')
    if cppr_prefix is not None:
      argv.append('CPPR_PREFIX='+cppr_prefix)
    argv.extend(['$bindir/scr_copy', '--cntldir', '$cntldir', '--id', '$dataset_id', '--prefix', '$prefixdir', '--buf', '$buf_size', '$crc_flag'])
    container_flag = scr_env.param.get('SCR_USE_CONTAINERS')
    if container_flag is not None and container_flag=='0':
      pass
    else:
      argv.append('--containers')
    argv.append('$downnodes_spaced')
    return argv
