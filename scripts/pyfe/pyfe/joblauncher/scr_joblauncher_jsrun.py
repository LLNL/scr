#! /usr/bin/env python3

# scr_joblauncher_jsrun.py
# The SCR_Joblauncher_jsrun class provides interpretation for the jsrun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_jsrun(SCR_Joblauncher_Base):
  def __init__(self,launcher='jsrun'):
    super(SCR_Joblauncher_jsrun, self).__init__(launcher=launcher)

  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    if len(launcher_args)==0:
      return []
    argv = [ self.conf['launcher'] ]
    if len(down_nodes)>0:
      argv.append('--exclude_hosts='+down_nodes)
    argv.extend(launcher_args)
    return argv
