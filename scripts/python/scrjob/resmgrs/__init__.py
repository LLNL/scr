"""Resource Manager classes provide an interface to the resource manager in
use.

The resource manager is determined by:
* The compile time constant, located in ../config.py

This file describes the classes which will be imported.

The base class, ResourceManager, should remain located at the top of this file.
The auto class, AutoResourceManager, should remain located at the bottom of this file.
New resource manager classes must be inserted in the middle section.
"""

from .resourcemanager import ResourceManager

from .slurm import SLURM
from .lsf import LSF
from .pbsalps import PBSALPS
from .flux import FLUX
#from .pmix import PMIX
#from .newfile import NewResourceManager

# The AutoResourceManager returns one of the above classes
from .auto import AutoResourceManager
