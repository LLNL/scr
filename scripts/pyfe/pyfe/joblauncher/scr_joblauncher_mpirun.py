#! /usr/bin/env python3

# scr_joblauncher_mpirun.py
# The SCR_Joblauncher_mpirun class provides interpretation for the mpirun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_mpirun(SCR_Joblauncher_Base):
  def __init__(self):
    self.conf = {}
    self.conf['launcher'] = 'mpirun'

  #def prepareforprerun(self):
  #def get_scr_end_time(self,jobid=None):

  def getlaunchargv(self,nodesarg='',launch_cmd=[]):
    if len(launch_cmd)==0:
      return []
    argv = [self.conf['launcher']]
    if len(nodesarg)>0:
      argv.extend(['--hostfile', nodesarg])
    argv.extend(launch_cmd)
    return argv
