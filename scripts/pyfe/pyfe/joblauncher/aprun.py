#! /usr/bin/env python3

# aprun.py
# The APRUN class provides interpretation for the aprun launcher

from pyfe.joblauncher import JobLauncher
from pyfe.scr_common import runproc

class APRUN(JobLauncher):
  def __init__(self,launcher='aprun'):
    super(APRUN, self).__init__(launcher=launcher)

  # a command to run immediately before prerun is ran
  # NOP srun to force every node to run prolog to delete files from cache
  # TODO: remove this if admins find a better place to clear cache
  def prepareforprerun(self):
    # NOP aprun to force every node to run prolog to delete files from cache
    # TODO: remove this if admins find a better place to clear cache
    argv=['aprun','/bin/hostname'] # ,'>','/dev/null']
    runproc(argv=argv)

  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    # ap run needs to specify nodes to use
    if len(launch_cmd)==0 or len(up_nodes)==0:
      return []
    argv = [ self.conf['launcher'] ]
    argv.extend([ '-L', up_nodes ])
    argv.extend(launcher_args)
    return argv
