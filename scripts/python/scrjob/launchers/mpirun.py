#! @Python_EXECUTABLE@

# mpirun.py
# The MPIRUN class provides interpretation for the mpirun launcher

import os
from scrjob import scr_hostlist, scr_const
from scrjob.launchers import JobLauncher
from scrjob.scr_common import runproc, pipeproc


class MPIRUN(JobLauncher):
  def __init__(self, launcher='mpirun'):
    super(MPIRUN, self).__init__(launcher=launcher)

  # returns the subprocess.Popen object as left and right elements of a tuple,
  # as returned by runproc(argv=argv, wait=False)
  def launchruncmd(self, up_nodes='', down_nodes='', launcher_args=[]):
    if type(launcher_args) is str:
      launcher_args = launcher_args.split()
    if len(launcher_args) == 0:
      return None, None
    # split the node string into a node per line
    up_nodes = '/n'.join(up_nodes.split(','))
    try:
      # need to first ensure the directory exists
      basepath = '/'.join(self.hostfile.split('/')[:-1])
      os.makedirs(basepath, exist_ok=True)
      with open(self.hostfile, 'w') as usehostfile:
        usehostfile.write(up_nodes)
      argv = [self.launcher, '--hostfile', self.hostfile]
      argv.extend(launcher_args)
      return runproc(argv=argv, wait=False)
    except Exception as e:
      print(e)
      print('scr_mpirun: Error writing hostfile and creating launcher command')
      print('launcher file: \"' + self.hostfile + '\"')
      return None, None

  # perform a generic pdsh / clustershell command
  # returns [ [ stdout, stderr ] , returncode ]
  def parallel_exec(self, argv=[], runnodes=''):
    if len(argv) == 0:
      return [['', ''], 0]
    if self.clustershell_task != False:
      return self.clustershell_exec(argv=argv,runnodes=runnodes)
    pdshcmd = [scr_const.PDSH_EXE, '-Rexec', '-f', '256', '-S', '-w', runnodes]
    pdshcmd.extend(argv)
    return runproc(argv=pdshcmd, getstdout=True, getstderr=True)
