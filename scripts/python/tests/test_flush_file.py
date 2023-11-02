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
# This will allow SCR to keep up to 6 datasets in cache.
# This will write 5 checkpoints (id=1,2,3,5,6)
# and one output dataset (id=4).
# The SCR library will have flushed 4 and 6 automatically.

print('--------------------------------------------------------')
print('This script should list 4 as an output set.')
print('The checkpoint ids should be listed in descending order.')
print('Datasets 4 and 6 should return False indicating that')
print('they do not need to be flushed.')
print('')
print('output: [\'4\']')
print('4 output.4 False PFS')
print('ckpt: [\'6\', \'5\', \'3\', \'2\', \'1\']')
print('6 ckpt.6 False PFS')
print('5 ckpt.5 True CACHE')
print('3 ckpt.3 True CACHE')
print('2 ckpt.2 True CACHE')
print('1 ckpt.1 True CACHE')
print('latest: 6')
print('--------------------------------------------------------')

import os
import sys
import time

from scrjob.cli import SCRFlushFile

if __name__ == '__main__':
    # in case flush file is on NFS, wait for a bit for it to show up
    time.sleep(2)

    pwd = os.getcwd()
    if len(sys.argv) == 2:
        pwd = sys.argv[1]

    ff = SCRFlushFile(pwd)

    dsets = ff.list_dsets_output()
    print('output:', dsets)
    for d in dsets:
        name = ff.name(d)
        needflush = ff.need_flush(d)
        location = ff.location(d)
        print(d, name, needflush, location)

    dsets = ff.list_dsets_ckpt()
    print('ckpt:', dsets)
    for d in dsets:
        name = ff.name(d)
        needflush = ff.need_flush(d)
        location = ff.location(d)
        print(d, name, needflush, location)

    print('latest:', ff.latest())

    time.sleep(2)
