"""Job Launcher classes provide an interface to specific launcher programs.

The job launcher is determined by:
* The particular scr_{ }run.py script which is invoked.
* The first argument when scr_run.py is directly invoked.

This file describes the classes which will be imported.

The base class, JobLauncher, should remain located at the top of this file.
The auto class, AutoJobLauncher, should remain located at the bottom of this file.

New job launcher classes must be inserted in the middle 'Job launcher' section
"""

# Parent JobLauncher class
from .joblauncher import JobLauncher

# Job launcher section
from .srun import SRUN
from .aprun import APRUN
from .jsrun import JSRUN
from .lrun import LRUN
from .mpirun import MPIRUN
from .flux import FLUX

# The AutoJobLauncher returns one of the above classes
from .auto import AutoJobLauncher
