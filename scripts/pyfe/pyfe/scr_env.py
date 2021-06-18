#! /usr/env python

# scr_env.py
# SCR_Env is class constructor for the specific environment classes
# the SCR_Env constructor takes an env argument
# env can be: 'SLURM', 'LSF', 'APRUN', 'PMIX', None
# if env is None we check scr_const.SCR_RESOURCE_MANAGER
# returns appropriate environment class according to env value
# returns an SCR_Env_Base object if the environment was not determined
# common methods shared between subclasses can be used from here
# (the __init__ is currently shared, all environments will use the super init)
# default functionality is given in the base class which subclasses may or may not override

from scr_env_slurm import SCR_Env_SLURM
from scr_env_lsf import SCR_Env_LSF
from scr_env_aprun import SCR_Env_APRUN
from scr_env_pmix import SCR_Env_PMIX
from scr_env_base import SCR_Env_Base
import scr_const

class SCR_Env:
  def __new__(cls,env=None):
    if env is None:
      env = scr_const.SCR_RESOURCE_MANAGER
    if env=='SLURM':
      return SCR_Env_SLURM()
    elif env=='LSF':
      return SCR_Env_LSF()
    elif env=='APRUN':
      return SCR_Env_APRUN()
    elif env=='PMIX':
      return SCR_Env_PMIX()
    return SCR_Env_Base()

if __name__ == '__main__':
  #scr_env = SCR_Env(env='SLURM')
  #scr_env = SCR_Env(env='LSF')
  #scr_env = SCR_Env(env='PMIX')
  #scr_env = SCR_Env(env='APRUN')
  scr_env = SCR_Env()
  scr_env.set_downnodes()
  print(type(scr_env))
  for key in scr_env.conf:
    print('scr_env.conf['+key+'] = \''+str(scr_env.conf[key])+'\'')

