#! /usr/bin/env python3

# auto.py

# AutoJobLauncher is class constructor for the specific job launchers
# The job launcher will handle differences between run commands

from pyfe import scr_const

from pyfe.joblauncher import (
    JobLauncher,
    APRUN,
    JSRUN,
    LRUN,
    MPIRUN,
    SRUN,
)

class AutoJobLauncher:
  def __new__(cls,launcher=None):
    if launcher is None:
      launcher = scr_const.SCR_LAUNCHER

    if launcher == 'srun':
      return SRUN()
    if launcher == 'jsrun':
      return JSRUN()
    if launcher == 'mpirun':
      return MPIRUN()
    if launcher == 'lrun':
      return LRUN()
    if launcher == 'aprun':
      return APRUN()
    return JobLauncher()

if __name__ == '__main__':
  joblauncher = JobLauncher()
  #joblauncher = JobLauncher(launcher='srun')
  #joblauncher = JobLauncher(launcher='jsrun')
  #joblauncher = JobLauncher(launcher='mpirun')
  #joblauncher = JobLauncher(launcher='lrun')
  #joblauncher = JobLauncher(launcher='aprun')
  print(type(joblauncher))
