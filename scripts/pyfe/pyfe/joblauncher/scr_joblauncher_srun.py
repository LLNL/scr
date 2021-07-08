#! /usr/bin/env python3

# scr_joblauncher_srun.py
# The SCR_Joblauncher_srun class provides interpretation for the srun launcher

from scr_common import runproc, pipeproc
from joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_srun(SCR_Joblauncher_Base):
  def __init__(self,launcher='srun'):
    super(SCR_Joblauncher_srun, self).__init__(launcher=launcher)

  # a command to run immediately before prerun is ran
  # NOP srun to force every node to run prolog to delete files from cache
  # TODO: remove this if admins find a better place to clear cache
  def prepareforprerun(self):
    # NOP srun to force every node to run prolog to delete files from cache
    # TODO: remove this if admins find a better place to clear cache
    argv=['srun','/bin/hostname'] # ,'>','/dev/null']
    runproc(argv=argv)

  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    if len(launcher_args)==0:
      return []
    argv = [self.conf['launcher']]
    if len(down_nodes)>0:
      argv.extend(['--exclude',down_nodes])
    argv.extend(launcher_args)
    return argv
