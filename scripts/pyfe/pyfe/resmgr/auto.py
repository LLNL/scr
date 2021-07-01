#! /usr/bin/env python3

# scr_resourcemgr.py

# SCR_Resourcemgr is class constructor for the specific resource managers
# the SCR_Resourcemgr constructor takes a resmgr argument
# resmgr can be: 'SLURM', 'LSF', 'APRUN', 'PMIX', None
# if resmgr is None we check scr_const.SCR_RESOURCE_MANAGER
# returns appropriate resource manager class according to resmgr value
# returns an SCR_Resourcemgr_Base object if the resource manager was not determined

from pyfe import scr_const

from pyfe.resmgr import *

class AutoResourceManager:
  def __new__(cls,resmgr=None):
    if resmgr is None:
      resmgr = scr_const.SCR_RESOURCE_MANAGER

    if resmgr=='SLURM':
      return SLURM()
    if resmgr=='LSF':
      return LSF()
    if resmgr=='APRUN':
      return APRUN()
    if resmgr=='PMIX':
      return PMIX()
    return ResourceManager()

if __name__ == '__main__':
  #resourcemgr = SCR_Resourcemgr()
  #print(type(resourcemgr))
  resourcemgr = AutoResourceManager(resmgr='SLURM')
  print(type(resourcemgr))
  #resourcemgr = SCR_Resourcemgr(resmgr='LSF')
  #print(type(resourcemgr))
  #resourcemgr = SCR_Resourcemgr(resmgr='APRUN')
  #print(type(resourcemgr))
  #resourcemgr = SCR_Resourcemgr(resmgr='PMIX')
  #print(type(resourcemgr))

