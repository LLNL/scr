# flux imports
try:
    import flux
except:
    pass

from scrjob.common import runproc
from scrjob.launchers import JobLauncher


class FLUX(JobLauncher):

    def __init__(self, launcher='flux'):
        super(FLUX, self).__init__(launcher=launcher)

        # connect to the running Flux instance
        try:
            self.flux = flux.Flux()
        except:
            raise ImportError(
                'Error importing flux, ensure that the flux daemon is running.'
            )

    def launch_run_cmd(self, up_nodes='', down_nodes='', launcher_args=[]):
        if type(launcher_args) is str:
            launcher_args = launcher_args.split(' ')
        if len(launcher_args) == 0:
            return None, None
        argv = [self.launcher]
        argv.extend(launcher_args)

        ### TODO: figure out how to exclude down_nodes

        # A jobspec is a yaml description of job and its resource requirements.
        # Building one lets us submit the job and get back the assigned jobid.
        argv.insert(2, '--dry-run')
        compute_jobreq, exitcode = runproc(argv=argv, getstdout=True)
        if compute_jobreq == None:
            return None, None

        # waitable=True is required by the call to wait_async() in waitonprocess()
        jobid = flux.job.submit(self.flux, compute_jobreq, waitable=True)
        return jobid, jobid

    def waitonprocess(self, proc, timeout=None):
        try:
            future = flux.job.wait_async(self.flux, proc)
            if timeout is None:
                (jobid, success, errstr) = future.get_status()
            else:
                (jobid, success, errstr) = future.wait_for(int(timeout))
                # TODO: verify return values of wait_for()
        except TimeoutError:
            # The process is still running, the timeout expired
            return False, None
        except Exception as e:
            # it can also throw an exception if there is no job to wait for
            print(e)
            return False, None

        if success == False:
            print(f'flux job {proc} failed: {errstr}')
        return True, success

    def kill_jobstep(self, jobstep=None):
        if jobstep is not None:
            try:
                flux.job.cancel(self.flux, jobstep)
                flux.job.wait_async(self.flux, jobstep)
            except Exception:
                # we could get 'invalid jobstep id' when the job has already terminated
                pass
