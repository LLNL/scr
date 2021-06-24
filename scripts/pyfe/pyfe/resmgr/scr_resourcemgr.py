#! /usr/bin/env python3

# scr_resourcemgr.py

# SCR_Resourcemgr is class constructor for the specific resource managers
# the SCR_Resourcemgr constructor takes a resmgr argument
# resmgr can be: 'SLURM', 'LSF', 'APRUN', 'PMIX', None
# if resmgr is None we check scr_const.SCR_RESOURCE_MANAGER
# returns appropriate resource manager class according to resmgr value
# returns an SCR_Resourcemgr_Base object if the resource manager was not determined

from pyfe import scr_const

from pyfe.resmgr.scr_resourcemgr_base import SCR_Resourcemgr_Base
from pyfe.resmgr.scr_resourcemgr_slurm import SCR_Resourcemgr_SLURM
from pyfe.resmgr.scr_resourcemgr_lsf import SCR_Resourcemgr_LSF
from pyfe.resmgr.scr_resourcemgr_aprun import SCR_Resourcemgr_APRUN
from pyfe.resmgr.scr_resourcemgr_pmix import SCR_Resourcemgr_PMIX

class SCR_Resourcemgr:
  def __new__(cls,resmgr=None):
    if resmgr is None:
      resmgr = scr_const.SCR_RESOURCE_MANAGER
    if resmgr=='SLURM':
      return SCR_Resourcemgr_SLURM()
    if resmgr=='LSF':
      return SCR_Resourcemgr_LSF()
    if resmgr=='APRUN':
      return SCR_Resourcemgr_APRUN()
    if resmgr=='PMIX':
      return SCR_Resourcemgr_PMIX()
    return SCR_Resourcemgr_Base()

if __name__ == '__main__':
  #resourcemgr = SCR_Resourcemgr()
  #print(type(resourcemgr))
  resourcemgr = SCR_Resourcemgr(resmgr='SLURM')
  print(type(resourcemgr))
  #resourcemgr = SCR_Resourcemgr(resmgr='LSF')
  #print(type(resourcemgr))
  #resourcemgr = SCR_Resourcemgr(resmgr='APRUN')
  #print(type(resourcemgr))
  #resourcemgr = SCR_Resourcemgr(resmgr='PMIX')
  #print(type(resourcemgr))

