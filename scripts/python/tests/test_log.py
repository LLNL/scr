# Setup to create a flush file for testing:
#
#   salloc -N1 -ppdebug
#
# Enable logging and then run the test:
#
#   setenv SCR_LOG_ENABLE=1
#   python test_log.py

print('--------------------------------------------------------')
print('We should get a file at .scr/log with contents like:')
print('')
print('2021-07-23T12:15:36: host=quartz1, jobid=7155341, event=test_event')
print(
    '2021-07-23T12:15:46: host=quartz1, jobid=7155341, event=test_event2, note=\"note2\", dset=100, name=\"ckpt.100\", secs=30.000000'
)
print('')
print(
    'Because of the sleep(10), the last entry should be about 10 seconds later'
)
print('--------------------------------------------------------')

import os
import sys
from datetime import datetime
import time

from scrjob.jobenv import JobEnv
from scrjob.resmgrs import AutoResourceManager
from scrjob.cli import SCRLog

time.sleep(2)

os.environ['SCR_LOG_ENABLE'] = '1'

jobenv = JobEnv()
user = jobenv.user()

resmgr = AutoResourceManager()
jobid = resmgr.job_id()

pwd = os.getcwd()
log = SCRLog(pwd, jobid, user)

timestamp = int(time.time())
datestring = str(datetime.now())
print('current timestamp =', timestamp, ' (', datestring, ')')
log.event('test_event')
time.sleep(10)
log.event('test_event2',
          dset=100,
          name='ckpt.100',
          note='note2',
          start=int(time.time()),
          secs=30)

try:
    with open('.scr/log', 'r') as infile:
        contents = infile.read()
        print('cat .scr/log')
        print(contents)
except:
    print('Error opening and reading .scr/log')

time.sleep(2)
