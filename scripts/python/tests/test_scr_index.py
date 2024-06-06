# Setup to create a flush file for testing
#
#   salloc -N1 -ppdebug
#
# Within allocation, run test_api with following settings:
#
#   export SCR_CACHE_BYPASS=0
#   export SCR_CACHE_SIZE=6
#   export SCR_FLUSH=6
#   srun -n1 -N1 ./test_api --output 4
#

print('--------------------------------------------------------')
print('This script should produce output like the following:')
print('')
print('python test_scr_index.py')
print('')
print('<scrjob.cli.scr_index.SCRIndex object at 0x2aaaac14b3c8>')
print('False')
print('True')
print('False')
print('True')
print('--------------------------------------------------------')

import os
import sys
import time

from scrjob.cli import SCRIndex

time.sleep(2)

pwd = os.getcwd()
scrindex = SCRIndex(pwd)
print(scrindex)

print(scrindex.current('ckpt.0'))
print(scrindex.current('ckpt.6'))

print(scrindex.build('0'))
print(scrindex.build('6'))

time.sleep(2)
