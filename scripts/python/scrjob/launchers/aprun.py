from time import sleep

from scrjob import config
from scrjob.launchers import JobLauncher
from scrjob.common import runproc


class APRUN(JobLauncher):

    def __init__(self, launcher='aprun'):
        super(APRUN, self).__init__(launcher=launcher)
        self.aprun_exe = 'aprun'

    # returns the process and PID of the launched process
    # as returned by runproc(argv=argv, wait=False)
    def launch_run(self, args, nodes=[], down_nodes=[]):
        # aprun needs to specify nodes to use
        if not nodes:
            return None, -1

        nodes_str = ','.join(nodes)

        argv = [self.aprun_exe]
        argv.extend(['-L', nodes_str])
        argv.extend(args)

        # TODO: ensure the Popen.terminate() works here too.
        if config.USE_JOBLAUNCHER_KILL != '1':
            return runproc(argv=argv, wait=False)

        proc = runproc(argv=argv, wait=False)[0]
        jobstepid = self.jobstep_id(pid=proc.pid)

        if jobstepid is not None:
            return proc, jobstepid
        else:
            return proc, proc

    def jobstep_id(self, pid=-1):
        # allow launched job to show in apstat
        sleep(10)
        output = runproc(['apstat', '-avv'], getstdout=True)[0].split('\n')

        nid = None
        try:
            with open('/proc/cray_xt/nid', 'r') as NIDfile:
                nid = NIDfile.read()[:-1]
        except:
            return None

        currApid = None
        pid = str(pid)
        for line in output:
            line = line.strip()
            fields = re.split('\s+', line)
            if len(fields) < 8:
                continue

            if fields[0].startswith('Ap'):
                currApid = fields[2][:-1]
            elif fields[1].startswith('Originator:'):
                #did we find the apid that corresponds to the pid?
                # also check to see if it was launched from this MOM node in case two
                # happen to have the same pid
                thisnid = fields[5][:-1]
                if thisnid == nid and fields[7] == pid:
                    break
                currApid = None

        return currApid

    # Only use akill to kill the jobstep if desired and jobstep_id was successful
    def kill_run(self, jobstep=None):
        # it looks like the Popen.terminate is working with srun
        if type(jobstep) is str:
            runproc(argv=['apkill', jobstep])
        else:
            super().kill_run(jobstep)
