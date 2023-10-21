#! /usr/bin/env python3

import os, sys

if 'scrjob' not in sys.path:
    sys.path.insert(0, '/'.join(os.path.realpath(__file__).split('/')[:-2]))
    import scrjob

import argparse
from datetime import datetime
from time import time

from scrjob.scr_test_runtime import SCR_Test_Runtime
from scrjob.scr_environment import SCR_Env
from scrjob.resmgrs import AutoResourceManager


def scr_prerun(scr_env=None, verbose=False):
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

    if scr_env is None or scr_env.resmgr is None:
        raise RuntimeError('scr_prerun: ERROR: Unknown environment')

    # check that we have all the runtime dependences we need
    if SCR_Test_Runtime(scr_env.resmgr.prerun_tests()) != 0:
        raise RuntimeError('scr_prerun: ERROR: runtime test failed')

    # create the .scr subdirectory in the prefix directory
    dir_scr = scr_env.dir_scr()
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p',
                        '--prefix',
                        metavar='<dir>',
                        type=str,
                        default=None,
                        help='Specify the prefix directory.')
    parser.add_argument('-v',
                        '--verbose',
                        action='store_true',
                        default=False,
                        help='Verbose output.')

    args = parser.parse_args()

    scr_env = SCR_Env(prefix=args.prefix)
    scr_env.resmgr = AutoResourceManager()

    scr_prerun(scr_env=scr_env, verbose=args.verbose)
