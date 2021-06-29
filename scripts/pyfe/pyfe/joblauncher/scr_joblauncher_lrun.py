#! /usr/bin/env python3

# scr_joblauncher_lrun.py
# The SCR_Joblauncher_lrun class provides interpretation for the lrun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_lrun(SCR_Joblauncher_Base):
  def __init__(self,launcher='lrun'):
    super(SCR_Joblauncher_lrun, self).__init__(launcher=launcher)

  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    if len(launcher_args)==0:
      return []
    argv = [ self.conf['launcher'] ]
    if len(down_nodes)>0:
      argv.append('--exclude_hosts='+down_nodes)
    argv.extend(launcher_args)
    return argv
