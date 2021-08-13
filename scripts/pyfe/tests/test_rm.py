#! /usr/bin/env python3

# Allocate a compute node
#
#   salloc -N1 -ppdebug -t 20
#   bsub -nnodes 1 -q pdebug -W 20 -Is /bin/bash
#

print('--------------------------------------------------------')
print('This script should produce output like:')
print('')
print('jobid: 7155402')
print('nodes: quartz13')
print('downnodes: ')
print('endsecs: 1627071990')
print('now: 1627068746 end: 1627071990 secs left: 3244')
print('')
print('The endtime should be in the future, and it')
print('should correspond to the allocation end time.')
print('--------------------------------------------------------')

import os, sys
import time
sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
import pyfe
from pyfe.resmgr import AutoResourceManager

time.sleep(2)

rm = AutoResourceManager()

print(str(type(rm)))
print("jobid:", rm.getjobid())
print("nodes:", rm.get_job_nodes())
print("downnodes:", rm.get_downnodes())

endtime = rm.get_scr_end_time()
print("endsecs:", endtime)

now = int(time.time())
secs = endtime - now
print("now:", now, "end:", endtime, "secs left:", secs)

time.sleep(2)
