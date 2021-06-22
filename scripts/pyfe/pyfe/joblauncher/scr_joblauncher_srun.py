#! /usr/env python

# scr_joblauncher_srun.py
# The SCR_Joblauncher_srun class provides interpretation for the srun launcher

from joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_srun(SCR_Joblauncher_Base):
  def __init__(self):
    self.launcher = 'srun'
    self.use_scr_watchdog=1
    self.nopargv = ['srun','/bin/hostname']
    self.scr_end_time = []
    self.scr_end_time.append(['scontrol', '--oneliner','show','job','$SLURM_JOBID'])
    self.scr_end_time.append(['perl','-n','-e','\'m/EndTime=(\\S*)/ and print $1\''])
    self.scr_end_time.append(['date','-d'])
    self.excludeargs = '--exclude $down_nodes'
    
    #super(SCR_Joblauncher_srun, self).__init__()
