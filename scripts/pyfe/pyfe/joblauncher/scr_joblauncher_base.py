#! /usr/bin/env python3

# scr_joblauncher_base.py
# SCR_Joblauncher_Base is the super class for the SCR_Joblauncher_ family

class SCR_Joblauncher_Base(object):
  def __init__(self,launcher=''):
    self.conf = {}
    self.conf['launcher'] = launcher
    self.conf['hostfile'] = ''

  # if a job launcher would like to perform any operations before scr_prerun
  def prepareforprerun(self):
    pass

  # some launchers use only up or down nodes.
  # the run_cmd or restart_cmd will be appended to launcher_args already
  def getlaunchargv(self,up_nodes='',down_nodes='',launcher_args=[]):
    # an empty argv will just immediately return
    # could return something like: ['echo','unknown launcher']
    return []

