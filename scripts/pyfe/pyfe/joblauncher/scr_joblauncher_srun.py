#! /usr/bin/env python3

# scr_joblauncher_srun.py
# The SCR_Joblauncher_srun class provides interpretation for the srun launcher

from pyfe.scr_common import runproc, pipeproc
from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_srun(SCR_Joblauncher_Base):
  def __init__(self):
    self.conf = {}
    self.conf['launcher'] = 'srun'

  # a command to run immediately before prerun is ran
  # NOP srun to force every node to run prolog to delete files from cache
  # TODO: remove this if admins find a better place to clear cache
  def prepareforprerun(self):
    # NOP srun to force every node to run prolog to delete files from cache
    # TODO: remove this if admins find a better place to clear cache
    argv=['srun','/bin/hostname'] # ,'>','/dev/null']
    runproc(argv=argv)

  def getlaunchargv(self,nodesarg='',launch_cmd=[]):
    if len(launch_cmd)==0:
      return []
    argv = [self.conf['launcher']]
    if len(nodesarg)>0:
      argv.extend(['--exclude',nodesarg])
    argv.extend(launch_cmd)
    return argv
