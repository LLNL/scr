"""Defines the Flux JobLauncher subclass."""

# flux imports
try:
    import flux
except ImportError:
    pass

from scrjob.common import runproc
from scrjob.launchers import JobLauncher


class FLUX(JobLauncher):
    """Handles launching and monitoring Flux jobs."""

    def __init__(self, launcher='flux'):
        super().__init__(launcher=launcher)
        self.flux_exe = 'flux'

        # connect to the running Flux instance
        try:
            self.flux = flux.Flux()
        except NameError as exc:
            raise ImportError(
                'Error importing flux, ensure that the flux daemon is running.'
            ) from exc

    def launch_run(self, args, nodes=[], down_nodes=[]):
        argv = [self.flux_exe]
        argv.extend(args)

        # TODO: figure out how to exclude down_nodes

        # A jobspec is a yaml description of job and its resource requirements.
        # Building one lets us submit the job and get back the assigned jobid.
        argv.insert(2, '--dry-run')
        compute_jobreq, _ = runproc(argv=argv, getstdout=True)
        if compute_jobreq is None:
            return None, None

        # waitable=True is required by the call to wait_async() in wait_run()
        jobid = flux.job.submit(self.flux, compute_jobreq, waitable=True)
        return jobid, jobid

    def wait_run(self, proc, timeout=None):
        try:
            future = flux.job.wait_async(self.flux, proc)
            if timeout is None:
                (_, success, errstr) = future.get_status()
            else:
                (_, success, errstr) = future.wait_for(int(timeout))
                # TODO: verify return values of wait_for()
        except TimeoutError:
            # The process is still running, the timeout expired
            return False, None
        except Exception as exc:
            # it can also throw an exception if there is no job to wait for
            print(exc)
            return False, None

        if success is False:
            print(f'flux job {proc} failed: {errstr}')
        return True, success

    def kill_run(self, jobstep=None):
        if jobstep is not None:
            try:
                flux.job.cancel(self.flux, jobstep)
                flux.job.wait_async(self.flux, jobstep)
            except Exception:
                # we could get 'invalid jobstep id' when the job has already terminated
                pass
