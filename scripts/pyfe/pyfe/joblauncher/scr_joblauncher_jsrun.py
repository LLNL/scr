#! /usr/bin/env python3

# scr_joblauncher_jsrun.py
# The SCR_Joblauncher_jsrun class provides interpretation for the jsrun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_jsrun(SCR_Joblauncher_Base):
  def __init__(self):
    self.launcher = 'jsrun'
    self.use_scr_watchdog = 0 # SCR_WATCHDOG not supported on LSF
    self.nopargv = None
    self.scr_end_time = [] # LSF/scr_env.in:181
    self.excludeargs = '--exclude_hosts=$down_nodes'
    #super(SCR_Joblauncher_jsrun, self).__init__()

