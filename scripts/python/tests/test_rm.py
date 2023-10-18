#! /usr/bin/env python3

# Allocate a compute node
#
#   salloc -N1 -ppdebug -t 20
#   bsub -nnodes 1 -q pdebug -W 20 -Is /bin/bash
#

print('--------------------------------------------------------')
print('This script should produce output like:')
print('')
print('<class \'pyfe.resmgr.flux.FLUX\'>')
print('jobid: 7155402')
print('nodes: quartz13')
print('downnodes: {}')
print('endsecs: 1627071990')
print('now: 1627068746 end: 1627071990 secs left: 3244')
print('')
print('The endtime will be 0 if it cannot be determined.')
print('Otherwise the endtime should be in the future, and it')
print('should correspond to the allocation end time.')
print('--------------------------------------------------------')

import os, sys
import time

sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
import scrjob
from scrjob.resmgrs import AutoResourceManager

time.sleep(2)

resmgr = AutoResourceManager()

print(str(type(resmgr)))
print("jobid:", resmgr.get_job_id())
print("nodes:", resmgr.get_job_nodes())
print("downnodes:", resmgr.get_down_nodes())

endtime = resmgr.get_end_time()
print("endsecs:", endtime)

now = int(time.time())
secs = endtime - now
if secs < 0:
    secs = 0
print("now:", now, "end:", endtime, "secs left:", secs)

time.sleep(2)
