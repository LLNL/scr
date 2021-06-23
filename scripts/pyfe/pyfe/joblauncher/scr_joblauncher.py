#! /usr/bin/env python

# scr_joblauncher.py

# SCR_Joblauncher is class constructor for the specific job launchers
# The job launcher will handle differences between run commands

from pyfe import scr_const

from pyfe.joblauncher.scr_joblauncher_base import SCR_Joblauncher_Base
from pyfe.joblauncher.scr_joblauncher_srun import SCR_Joblauncher_srun
from pyfe.joblauncher.scr_joblauncher_jsrun import SCR_Joblauncher_jsrun
from pyfe.joblauncher.scr_joblauncher_mpirun import SCR_Joblauncher_mpirun

class SCR_Joblauncher:
  def __new__(cls,launcher=None):
    if launcher is None:
      launcher = scr_const.SCR_LAUNCHER
    if launcher == 'srun':
      return SCR_Joblauncher_srun()
    if launcher == 'jsrun':
      return SCR_Joblauncher_jsrun()
    if launcher == 'mpirun':
      return SCR_Joblauncher_mpirun()
    return SCR_Joblauncher_Base()

if __name__ == '__main__':
  joblauncher = SCR_Joblauncher()
  #joblauncher = SCR_Joblauncher(launcher='srun')
  #joblauncher = SCR_Joblauncher(launcher='jsrun')
  #joblauncher = SCR_Joblauncher(launcher='mpirun')
  print(type(joblauncher))
