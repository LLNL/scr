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

  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    if len(launch_cmd)==0:
      return []
    argv = [self.conf['launcher']]
    if len(down_nodes)>0:
      argv.extend(['--exclude',down_nodes])
    argv.extend(launcher_args)
    return argv
