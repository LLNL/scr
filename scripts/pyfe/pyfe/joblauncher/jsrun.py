#! /usr/bin/env python3

# jsrun.py
# The jsrun class provides interpretation for the jsrun launcher

from pyfe.joblauncher import JobLauncher

class JSRUN(JobLauncher):
  def __init__(self,launcher='jsrun'):
    super(JSRUN, self).__init__(launcher=launcher)

  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    if len(launcher_args)==0:
      return []
    argv = [ self.conf['launcher'] ]
    if len(down_nodes)>0:
      argv.append('--exclude_hosts='+down_nodes)
    argv.extend(launcher_args)
    return argv
