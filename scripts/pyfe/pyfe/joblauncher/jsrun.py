#! /usr/bin/env python3

# jsrun.py
# The jsrun class provides interpretation for the jsrun launcher

from time import sleep

from pyfe import scr_const
from pyfe.joblauncher import JobLauncher
from pyfe.scr_common import runproc, pipeproc


class JSRUN(JobLauncher):
  def __init__(self, launcher='jsrun'):
    super(JSRUN, self).__init__(launcher=launcher)

  # returns the process and PID of the launched process
  # as returned by runproc(argv=argv, wait=False)
  def launchruncmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    if type(launcher_args) is str:
      launcher_args = launcher_args.split()
    if len(launcher_args) == 0:
      return None, -1
    argv = [self.launcher]
    if len(down_nodes) > 0:
      argv.append('--exclude_hosts ' + down_nodes)
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
                     downnodes_spaced='',
                     cntldir='',
                     dataset_id='',
                     prefixdir='',
                     buf_size='',
                     crc_flag=''):
    argv = [
        prog, '--cntldir', cntldir, '--id', dataset_id, '--prefix', prefixdir,
        '--buf', buf_size, crc_flag, downnodes_spaced
    ]
    output = self.parallel_exec(argv=argv, runnodes=upnodes,
                                use_dshbak=False)[0]
    return output

  def killsprocess(self):
    return True

  def get_jobstep_id(self,attempts=0):
    sleep(5)
    jobstepid = -1
    output = runproc(['jslist'], getstdout=True)[0]
    for line in output.split('\n'):
      if 'Running' not in line:
        continue
      line = line.split()
      if int(line[0]) > jobstepid:
        jobstepid = int(line[0])
    if jobstepid == -1 and attempts == 0:
      return self.get_jobstep_id(attempts=1)
    return jobstepid

  def scr_kill_jobstep(self, jobstepid=None):
    if jobstepid is not None:
      runproc(argv=['jskill', str(jobstepid)])
