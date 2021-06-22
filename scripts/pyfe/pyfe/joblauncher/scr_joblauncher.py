#! /usr/bin/env python

# scr_joblauncher.py

# SCR_Joblauncher is class constructor for the specific job launchers
# The job launcher will handle differences between run commands

import scr_const

from joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base
from joblauncher.scr_joblauncher_srun import SCR_Joblauncher_srun

class SCR_Joblauncher:
  def __new__(cls,launcher=None):
    if launcher is None:
      launcher = scr_const.SCR_RESOURCE_MANAGER # need to determine which launcher to use
    if launcher == 'srun':
      return SCR_Joblauncher_srun()
    return SCR_Joblauncher_Base()

if __name__ == '__main__':
  #joblauncher = SCR_Joblauncher()
  joblauncher = SCR_Joblauncher(launcher='srun')
  print(type(joblauncher))
