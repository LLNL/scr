#! /usr/bin/env python3

# scr_joblauncher_base.py
# SCR_Joblauncher_Base is the super class for the SCR_Joblauncher_ family

class SCR_Joblauncher_Base(object):
  def __init__(self):
    self.conf = {}
    self.conf['launcher'] = None
    pass

  # if a job launcher would like to perform any operations before scr_prerun
  def prepareforprerun(self):
    pass

  def get_scr_end_time(self,jobid=None):
    return None
