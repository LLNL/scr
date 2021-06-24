#! /usr/bin/env python3

# scr_joblauncher_jsrun.py
# The SCR_Joblauncher_jsrun class provides interpretation for the jsrun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_jsrun(SCR_Joblauncher_Base):
  def __init__(self):
    self.conf = {}
    self.conf['launcher'] = 'jsrun'

  def prepareforprerun(self):
    pass

  def get_scr_end_time(self,jobid=None):
    return None