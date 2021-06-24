#! /usr/bin/env python3

# scr_joblauncher_lrun.py
# The SCR_Joblauncher_lrun class provides interpretation for the lrun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_lrun(SCR_Joblauncher_Base):
  def __init__(self):
    self.conf = {}
    self.conf['launcher'] = 'lrun'

  def prepareforprerun(self):
    pass

  def get_scr_end_time(self,jobid=None):
    return None
