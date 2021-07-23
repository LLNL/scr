#! /usr/bin/env python3

# lrun.py
# The LRUN class provides interpretation for the lrun launcher

from pyfe import scr_const
from pyfe.joblauncher import JobLauncher
from pyfe.scr_common import runproc, pipeproc


class LRUN(JobLauncher):
  def __init__(self, launcher='lrun'):
    super(LRUN, self).__init__(launcher=launcher)

  # returns the process and PID of the launched process
  # as returned by runproc(argv=argv, wait=False)
  def launchruncmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    if len(launcher_args) == 0:
      return None, -1
    argv = [self.launcher]
    if len(down_nodes) > 0:
      argv.append('--exclude_hosts=' + down_nodes)
    argv.extend(launcher_args)
    return runproc(argv=argv, wait=False)

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes='', use_dshbak=True):
    if len(argv) == 0:
      return [['', ''], 0]
    if self.clustershell_task != False:
      return self.clustershell_exec(argv=argv,
                                    runnodes=runnodes,
                                    use_dshbak=use_dshbak)
    pdshcmd = [scr_const.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', runnodes]
    pdshcmd.extend(argv)
    if use_dshbak:
      argv = [pdshcmd, [scr_const.DSHBAK_EXE, '-c']]
      return pipeproc(argvs=argv, getstdout=True, getstderr=True)
    return runproc(argv=pdshcmd, getstdout=True, getstderr=True)

  # perform the scavenge files operation for scr_scavenge
  # uses either pdsh or clustershell
  # returns a list -> [ 'stdout', 'stderr' ]
  def scavenge_files(self,
                     prog='',
                     upnodes='',
                     downnodes='',
                     cntldir='',
                     dataset_id='',
                     prefixdir='',
                     buf_size='',
                     crc_flag=''):
    upnodes, downnodes_spaced = self.resmgr.get_scavenge_nodelists(
        upnodes=upnodes, downnodes=downnodes)
    argv = [
        prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir,
        '--buf', buf_size, crc_flag, downnodes_spaced
    ]
    output = self.parallel_exec(argv=argv, runnodes=upnodes,
                                use_dshbak=False)[0]
    return output
