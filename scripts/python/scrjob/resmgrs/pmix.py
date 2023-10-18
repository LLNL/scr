'''
# pmix.py
# PMIX is a subclass if ResourceManager

import os
from scrjob import scr_const
from scrjob.scr_common import runproc, pipeproc
from scrjob.resmgrs import nodetests, ResourceManager

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
  def get_jobstep_id(self,user='',pid=-1):
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
    val = os.environ.get('PMIX_NODELIST')
    if val is not None:
      node_list = val.split(',')
      nodeset = self.compress_hosts(node_list)
      return nodeset
    return None

  def down_nodes(self):
    # if the resource manager knows any nodes to be down out of the job's
    # nodeset, print this list in 'atlas[30-33,35,45-53]' form
    # if there are none, print nothing, not even a newline
    # CALL SCR_ENV_HELPER FOR PMIX - THIS IS A TODO AS PMIX DOESN'T SUPPORT IT YET
    #if (0) {
    #  my $nodeset = ""; #get nodeset with pmixhelper
    return None

  def scr_kill_jobstep(self,jobid=-1):
    print('pmix does not support this')
    return 1

  # return a hash to define all unavailable (down or excluded) nodes and reason
  def list_down_nodes_with_reason(self,nodes=[], scr_env=None, free=False, cntldir_string=None, cachedir_string=None):
    unavailable = {}
    ### is theres way to get a list of down nodes in pmix?
    #unavailable = nodetests.list_resmgr_down_nodes(nodes=nodes, resmgr_nodes=self.expand_hosts(self.down_nodes()))
    nextunavail = nodetests.list_nodes_failed_ping(nodes=nodes)
    unavailable.update(nextunavail)
    if scr_env is not None and scr_env.param is not None:
      exclude_nodes = self.expand_hosts(scr_env.param.get('SCR_EXCLUDE_NODES'))
      nextunavail = nodetests.list_param_excluded_nodes(nodes=self.expand_hosts(nodes), exclude_nodes=exclude_nodes)
      unavailable.update(nextunavail)
      # assert scr_env.resmgr == self
      nextunavail = nodetests.check_dir_capacity(nodes=nodes, free=free, scr_env=scr_env, cntldir_string=cntldir_string, cachedir_string=cachedir_string)
      unavailable.update(nextunavail)
    return unavailable

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if len(argv)==0:
      return [ [ '', '' ], 0 ]
    if self.clustershell is not None:
      return self.clustershell_exec(argv=argv, runnodes=runnodes, use_dshbak=use_dshbak)
    pdshcmd = [scr_const.PDSH_EXE, '-f', '256', '-S', '-w', runnodes]
    pdshcmd.extend(argv)
    if use_dshbak:
      argv = [ pdshcmd, [scr_const.DSHBAK_EXE, '-c'] ]
      return pipeproc(argvs=argv,getstdout=True,getstderr=True)
    return runproc(argv=pdshcmd,getstdout=True,getstderr=True)

  # perform the scavenge files operation for scr_scavenge
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self, prog='', upnodes='', downnodes='', cntldir='', dataset_id='', prefixdir='', buf_size='', crc_flag=''):
    argv = []
    cppr_lib = scr_const.CPPR_LDFLAGS
    if cppr_lib.startswith('-L'):
      cppr_lib = cppr_lib[2:]
      ldpath = os.environ.get('LD_LIBRARY_PATH')
      if ldpath is None:
        ldpath=''
      argv.append('LD_LIBRARY_PATH='+cpprlib+':'+ldpath)
    cppr_prefix = os.environ.get('CPPR_PREFIX')
    if cppr_prefix is not None:
      argv.append('CPPR_PREFIX='+cppr_prefix)
    argv.extend([prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir, '--buf', buf_size, crc_flag])
    argv.append(downnodes_spaced)
    output = self.parallel_exec(argv=argv,runnodes=self.job_nodes(),use_dshbak=False)[0]
    return output
'''
