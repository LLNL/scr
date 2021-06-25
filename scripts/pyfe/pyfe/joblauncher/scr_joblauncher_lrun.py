#! /usr/bin/env python3

# scr_joblauncher_lrun.py
# The SCR_Joblauncher_lrun class provides interpretation for the lrun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_lrun(SCR_Joblauncher_Base):
  def __init__(self):
    self.conf = {}
    self.conf['launcher'] = 'lrun'

  #def prepareforprerun(self):

  def getlaunchargv(self,nodesarg='',launch_cmd=[]):
    if len(launch_cmd)==0:
      return []
    argv = [ self.conf['launcher'] ]
    if len(nodesarg)>0:
      argv.append('--exclude_hosts='+nodesarg)
    argv.extend(launch_cmd)
    return argv
