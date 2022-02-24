#! /usr/bin/env python3
"""
Resource Manager classes provide an interface to the resource manager in use.

The resource manager is determined by:
* The compile time constant, located in ../scr_const.py

This file describes the classes which will be imported.

The base class, ResourceManager, should remain located at the top of this file.
The auto class, AutoResourceManager, should remain located at the bottom of this file.
New resource manager classes must be inserted in the middle section.
"""

####
# Parent ResourceManager class
from .resourcemanager import ResourceManager
####
####
# Resource manager section
from .slurm import SLURM
from .lsf import LSF
from .pbsalps import PBSALPS
#from .pmix import PMIX
#from .newfile import NewResourceManager

####
####
# The AutoResourceManager returns one of the above classes
from .auto import AutoResourceManager
####
