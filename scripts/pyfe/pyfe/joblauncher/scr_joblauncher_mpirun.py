#! /usr/env python

# scr_joblauncher_mpirun.py
# The SCR_Joblauncher_mpirun class provides interpretation for the mpirun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_mpirun(SCR_Joblauncher_Base):
  def __init__(self):
    self.launcher = 'mpirun'
    self.use_scr_watchdog = 1
    self.nopargv = None
    self.scr_end_time = [] # LSF/scr_env.in:181
    self.excludeargs = '--exclude_hosts=$down_nodes'
    #super(SCR_Joblauncher_mpirun, self).__init__()
