import os
from datetime import datetime
from time import time

from scrjob.scr_test_runtime import SCR_Test_Runtime


def prerun(jobenv=None, verbose=False):
    """This is called to set up an SCR run.

    Calls SCR_Test_Runtime with a list of tests provided by the resmgr.
    Ensures the .scr directory exists.
    Removes existing flush or nodes files.

    Returns
    -------
    None
    """

    # bail out if not enabled
    val = os.environ.get('SCR_ENABLE')
    if val == '0':
        return

    start_time = datetime.now()
    start_secs = int(time())
    if verbose:
        print('scr_prerun: Started: ' + str(start_time))

    if jobenv is None or jobenv.resmgr is None:
        raise RuntimeError('scr_prerun: ERROR: Unknown environment')

    # check that we have all the runtime dependences we need
    if SCR_Test_Runtime(jobenv.resmgr.prerun_tests()) != 0:
        raise RuntimeError('scr_prerun: ERROR: runtime test failed')

    # create the .scr subdirectory in the prefix directory
    dir_scr = jobenv.dir_scr()
    os.makedirs(dir_scr, exist_ok=True)

    # TODO: It would be nice to clear the cache and control directories
    # here in preparation for the run.  However, a simple rm -rf is too
    # dangerous, since it's too easy to accidentally specify the wrong
    # base directory.

    # clear any existing flush or nodes files
    # NOTE: we *do not* clear the halt file, since the user may have
    # requested the job to halt
    # remove files: ${pardir}/.scr/{flush.scr,nodes.scr}
    try:
        os.remove(os.path.join(dir_scr, 'flush.scr'))
    except:
        pass

    try:
        os.remove(os.path.join(dir_scr, 'nodes.scr'))
    except:
        pass

    # report timing info
    end_time = datetime.now()
    run_secs = int(time()) - start_secs
    if verbose:
        print('scr_prerun: Ended: ' + str(end_time))
        print('scr_prerun: secs: ' + str(run_secs))
