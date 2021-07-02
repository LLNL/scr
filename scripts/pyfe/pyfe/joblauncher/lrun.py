#! /usr/bin/env python3

# lrun.py
# The LRUN class provides interpretation for the lrun launcher

from pyfe.joblauncher import JobLauncher

class LRUN(JobLauncher):
  def __init__(self,launcher='lrun'):
    super(LRUN, self).__init__(launcher=launcher)

  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    if len(launcher_args)==0:
      return []
    argv = [ self.conf['launcher'] ]
    if len(down_nodes)>0:
      argv.append('--exclude_hosts='+down_nodes)
    argv.extend(launcher_args)
    return argv
