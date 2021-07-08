#! /usr/bin/env python3

####
# Parent ResourceManager class
from .resourcemanager import ResourceManager
###
####
# Resource manager section
from .slurm import SLURM
from .lsf import LSF
from .pbsalps import PBSALPS
from .pmix import PMIX



###
####
# The AutoResourceManager returns one of the above classes
from .auto import AutoResourceManager
#####
