#! /usr/bin/env python3

# scr_resourcemgr.py

# SCR_ResourceMgr is class constructor for the specific resource managers
# the SCR_ResourceMgr constructor takes a resmgr argument
# resmgr can be: 'SLURM', 'LSF', 'APRUN', 'PMIX', None
# if resmgr is None we check scr_const.SCR_RESOURCE_MANAGER
# returns appropriate resource manager class according to resmgr value
# returns an SCR_ResourceMgr_Base object if the resource manager was not determined

from pyfe import scr_const

from pyfe.resmgr.scr_resourcemgr_base import SCR_ResourceMgr_Base
from pyfe.resmgr.scr_resourcemgr_slurm import SCR_ResourceMgr_SLURM
from pyfe.resmgr.scr_resourcemgr_lsf import SCR_ResourceMgr_LSF
from pyfe.resmgr.scr_resourcemgr_aprun import SCR_ResourceMgr_APRUN
from pyfe.resmgr.scr_resourcemgr_pmix import SCR_ResourceMgr_PMIX

class SCR_ResourceMgr:
  def __new__(cls,resmgr=None):
    if resmgr is None:
      resmgr = scr_const.SCR_RESOURCE_MANAGER
    if resmgr=='SLURM':
      return SCR_ResourceMgr_SLURM()
    if resmgr=='LSF':
      return SCR_ResourceMgr_LSF()
    if resmgr=='APRUN':
      return SCR_ResourceMgr_APRUN()
    if resmgr=='PMIX':
      return SCR_ResourceMgr_PMIX()
    return SCR_ResourceMgr_Base()

if __name__ == '__main__':
  resourcemgr = SCR_ResourceMgr()
  #resourcemgr = SCR_ResourceMgr(resmgr='srun')
  #resourcemgr = SCR_ResourceMgr(resmgr='jsrun')
  #resourcemgr = SCR_ResourceMgr(resmgr='mpirun')
  print(type(resourcemgr))
