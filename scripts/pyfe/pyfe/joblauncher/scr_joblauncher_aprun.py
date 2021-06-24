#! /usr/bin/env python3

# scr_joblauncher_aprun.py
# The SCR_Joblauncher_aprun class provides interpretation for the aprun launcher

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base

class SCR_Joblauncher_aprun(SCR_Joblauncher_Base):
  def __init__(self):
    self.conf = {}
    self.conf['launcher'] = 'aprun'

  # a command to run immediately before prerun is ran
  # NOP srun to force every node to run prolog to delete files from cache
  # TODO: remove this if admins find a better place to clear cache
  def prepareforprerun(self):
    # NOP aprun to force every node to run prolog to delete files from cache
    # TODO: remove this if admins find a better place to clear cache
    argv=['aprun','/bin/hostname'] # ,'>','/dev/null']
    runproc(argv=argv)

  def get_scr_end_time(self,jobid=None):
    return None
