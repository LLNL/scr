#! /usr/bin/env python3

#srun.py
# The SRUN class provides interpretation for the srun launcher

from pyfe.scr_common import runproc, pipeproc
from pyfe.joblauncher import JobLauncher

class SRUN(JobLauncher):
  def __init__(self,launcher='srun'):
    super(SRUN, self).__init__(launcher=launcher)

  # a command to run immediately before prerun is ran
  # NOP srun to force every node to run prolog to delete files from cache
  # TODO: remove this if admins find a better place to clear cache
  def prepareforprerun(self):
    # NOP srun to force every node to run prolog to delete files from cache
    # TODO: remove this if admins find a better place to clear cache
    argv=['srun','/bin/hostname'] # ,'>','/dev/null']
    runproc(argv=argv)

  # returns the process and PID of the launched process
  # as returned by runproc(argv=argv, wait=False)
  def launchruncmd(self,up_nodes='',down_nodes='',launcher_args=[]):
    if len(launcher_args)==0:
      return None, -1
    argv = [self.conf['launcher']]
    if len(down_nodes)>0:
      argv.extend(['--exclude',down_nodes])
    argv.extend(launcher_args)
    return runproc(argv=argv, wait=False)

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if len(argv)==0:
      return [ [ '', '' ], 0 ]
    if self.conf['ClusterShell'] == True:
      return self.clustershell_exec(argv=argv, runnodes=runnodes, use_dshbak=use_dshbak)
    pdshcmd = [scr_const.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', runnodes]
    pdshcmd.extend(argv)
    if use_dshbak:
      argv = [ pdshcmd, [scr_const.DSHBAK_EXE, '-c'] ]
      return pipeproc(argvs=argv,getstdout=True,getstderr=True)
    return runproc(argv=pdshcmd,getstdout=True,getstderr=True)

  # perform the scavenge files operation for scr_scavenge
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self, prog='', upnodes='', downnodes='', cntldir='', dataset_id='', prefixdir='', buf_size='', crc_flag=''):
    upnodes, downnodes_spaced = self.conf['resmgr'].get_scavenge_nodelists(upnodes=upnodes, downnodes=downnodes)
    argv = [prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir, '--buf', buf_size, crc_flag, downnodes_spaced]
    output = self.parallel_exec(argv=argv,runnodes=upnodes,use_dshbak=False)[0]
    return output
