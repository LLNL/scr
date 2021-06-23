#! /usr/bin/env python3

# scr_joblauncher_srun.py
# The SCR_Joblauncher_srun class provides interpretation for the srun launcher

from pyfe.scr_common import runproc, pipeproc
from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_srun(SCR_Joblauncher_Base):
  def __init__(self):
    self.launcher = 'srun'
    self.use_scr_watchdog=1
    self.nopargv = ['srun','/bin/hostname']
    self.excludeargs = '--exclude $down_nodes' 
    #super(SCR_Joblauncher_srun, self).__init__()

  # launchexclude
  # takes string of down nodes
  # returns the start of argv for the launcher command
  @staticmethod
  def launchexclude_argv(down_nodes=''):
    if down_nodes is None or len(down_nodes)==0:
      return ['srun']
    return ['srun', '--exclude', down_nodes]

  # a command to run immediately before prerun is ran
  # NOP srun to force every node to run prolog to delete files from cache
  # TODO: remove this if admins find a better place to clear cache
  @staticmethod
  def pre_prerun_argv():
    return ['srun', '/bin/hostname']

  @staticmethod
  def get_scr_end_time(jobid=None):
    if jobid is None:
      jobid = self.getjobid()
      if jobid is None:
        return None
    argv = []
    argv.append(['scontrol','--oneliner','show','job',jobid])
    argv.append(['perl','-n','-e','\'m/EndTime=(\\S*)/ and print $1\''])
    output = pipeproc(argvs=argv,getstdout=True)[0]
    argv = ['date','-d',output.rstrip()]
    output = runproc(argv=argv,getstdout=True)[0]
    return output.rstrip()

