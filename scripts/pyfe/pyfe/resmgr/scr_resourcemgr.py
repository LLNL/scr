#! /usr/bin/env python3

# scr_resourcemgr.py

# SCR_ResourceMgr is class constructor for the specific resource managers

from pyfe import scr_const

from pyfe.resmgr.scr_resourcemgr_base import SCR_ResourceMgr_Base
from pyfe.resmgr.scr_resourcemgr_srun import SCR_ResourceMgr_srun
from pyfe.resmgr.scr_resourcemgr_jsrun import SCR_ResourceMgr_jsrun
from pyfe.resmgr.scr_resourcemgr_mpirun import SCR_ResourceMgr_mpirun

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
