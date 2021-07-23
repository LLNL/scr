#! /usr/bin/env python3
"""
joblauncher/auto.py
AutoJobLauncher is called to return the appropriate JobLauncher class

This is the class called to obtain an instance of a job launcher.

To add a new job launcher:
* Insert the new class name at the end of pyfe.joblauncher import statement
* Insert the condition and return statement for your new launcher within __new__
"""

from pyfe.joblauncher import (
    JobLauncher,
    APRUN,
    JSRUN,
    LRUN,
    MPIRUN,
    SRUN,
)


class AutoJobLauncher:
  def __new__(cls, launcher=None):
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
  joblauncher = AutoJobLauncher()
  #joblauncher = AutoJobLauncher(launcher='srun')
  #joblauncher = AutoJobLauncher(launcher='jsrun')
  #joblauncher = AutoJobLauncher(launcher='mpirun')
  #joblauncher = AutoJobLauncher(launcher='lrun')
  #joblauncher = AutoJobLauncher(launcher='aprun')
  print(type(joblauncher))
