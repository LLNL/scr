import os, re
from time import time

from scrjob.common import runproc
from scrjob.resmgrs import ResourceManager


class LSF(ResourceManager):
    # init initializes vars from the environment
    def __init__(self):
        super(LSF, self).__init__(resmgr='LSF')

    # get LSF jobid
    def job_id(self):
        return os.environ.get('LSB_JOBID')

    # get node list
    def job_nodes(self):
        hostfile = os.environ.get('LSB_DJOB_HOSTFILE')
        if hostfile is not None:
            try:
                # make a list from the set -> make a set from the list -> file.readlines().rstrip('\n')
                # get a list of lines without newlines and skip the first line
                lines = []
                with open(hostfile, 'r') as f:
                    lines = [line.strip() for line in f.readlines()]

                if len(lines) == 0:
                    raise RuntimeError('LSF: ERROR: $LSB_DJOB_HOSTFILE empty')

                # get a set of unique hostnames
                hosts = list(set(lines[1:]))
                return hosts
            except Exception as e:
                # failed to read file
                print('LSF: ERROR: failed to process $LSB_DJOB_HOSTFILE')
                print(e)
                raise e

        return None

        # fall back to try LSB_HOSTS
        hosts = os.environ.get('LSB_HOSTS')
        if hosts is not None:
            hosts = hosts.split(' ')
            hosts = list(set(hosts[1:]))
        return hosts

    def down_nodes(self):
        # TODO : any way to get list of down nodes in LSF?
        return {}

    def end_time(self):
        # run bjobs to get time remaining in current allocation
        bjobs, rc = runproc("bjobs -o time_left", getstdout=True)
        if rc != 0:
            return 0

        # parse bjobs output
        lines = bjobs.split('\n')
        for line in lines:
            line = line.strip()

            # skip empty lines
            if len(line) == 0:
                continue

            # the following is printed if there is no limit
            #   bjobs -o 'time_left'
            #   TIME_LEFT
            #   -
            # look for the "-", in this case,
            # return -1 to indicate there is no limit
            if line.startswith('-'):
                # no limit
                return -1

            # the following is printed if there is a limit
            #   bjobs -o 'time_left'
            #   TIME_LEFT
            #   0:12 L
            # look for a line like "0:12 L",
            # avoid matching the "L" since other characters can show up there
            pieces = re.split(r'(^\s*)(\d+):(\d+)\s+', line)
            if len(pieces) < 3:
                continue
            #print(line)

            # get current secs since epoch
            secs_now = int(time())

            # compute seconds left in job
            hours = int(pieces[2])
            mins = int(pieces[3])
            secs_remaining = ((hours * 60) + mins) * 60

            secs = secs_now + secs_remaining
            return secs

        # had a problem executing bjobs command
        return 0
