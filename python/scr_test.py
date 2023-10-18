"""Run basic behavior tests for python interface."""

import os
import sys
from mpi4py import MPI
import scr

# get the test number from the command line
test = int(sys.argv[1])

# get the MPI rank of this process
rank = MPI.COMM_WORLD.rank

# assume we'll start the job from beginning (timestep 1)
timestep = 1

# optionally set and get SCR parameters with scr.config() before scr.init()
val = scr.config("SCR_DEBUG")
if test == 0: assert val is None, "SCR_DEBUG should not be set on first run"
if test > 0:
    assert val is None, "SCR_DEBUG should assume its default value even after being changed in an earlier run"

val = scr.config("SCR_DEBUG=1")
assert val is None, "scr.config should not return a value when setting a param"

val = scr.config("SCR_DEBUG")
assert val == "1", "SCR_DEBUG should now be 1"
print("SCR_DEBUG:", val)

# enable scr.need_checkpoint() so that it returns True
scr.config("SCR_CHECKPOINT_INTERVAL=1")

# check that scr.init can throw an exception
if test == 5:
    scr.config("SCR_ENABLE=0")
    try:
        scr.init()
        assert False, "scr.init should throw an exception if SCR_Init returns an error"
    except:
        # success, but nothing else we can do in this run
        quit()

# initialize library, rebuild cached datasets, fetch latest checkpoint
val = scr.init()
assert val is None, "scr.init should always return None"

# attempt to restart from a previous checkpoint
attempt = 0
while True:
    # see if we have a restart to load
    name = scr.have_restart()
    if test == 0:
        assert name is None, "Should not have a checkpoint to read on first run"
    if test > 0:
        assert type(
            name
        ) is str, "scr.have_restart should return checkpoint name as a str on later runs"
    if not name:
        # no restart to load, give up
        break

    attempt += 1

    # got a restart to load, start restart phase and get its name
    name = scr.start_restart()
    assert name is not None, "Should always get a valid name after have_restart"
    if test == 1:
        assert name == "timestep_2", "Should restart from timestep 2 after one run"
    if test == 2:
        assert name == "timestep_4", "Should restart from timestep 4 after two runs"
    if test == 3 and attempt == 1:
        assert name == "timestep_6", "Should restart from timestep 6 after three runs and first attempt"
    if test == 3 and attempt == 2:
        assert name == "timestep_5", "Should restart from timestep 5 after three runs and second attempt"
    if test == 4:
        assert name == "timestep_7", "Should restart from timestep 7 after four runs"
    print("restart name: ", name)

    # extract timestep value from name like "timestep_10"
    timestep = int(name[9:])

    # build file name for this timestep and rank
    fname = 'ckpt_' + str(timestep) + '_' + str(rank) + '.txt'

    # ask scr for path from which to read our file
    newfname = scr.route_file(fname)
    assert newfname is not None, "scr.route_file should return a valid filename in restart"
    print("restart route: ", fname, "-->", newfname)

    # check that route file throws an exception for a bad file on restart
    try:
        scr.route_file("file_that_does_not_exist.txt")
        assert False, "scr.route_file should throw an exception if given a file it doesn't know about"
    except:
        pass

    # read checkpoint file
    valid = 1
    try:
        with open(newfname, "r") as f:
            data = f.readlines()
            print(str(data))
    except:
        # failed to read file
        valid = 0

    # test: fake a bad file on rank 0 for timestep 6
    if rank == 0 and timestep == 6:
        valid = 0

    # check whether everyone read their checkpoint file successfully
    rc = scr.complete_restart(valid)
    if timestep == 6:
        assert rc is False, "scr.complete_restart should return False when any rank sets valid=False"
    if timestep != 6:
        assert rc is True, "scr.complete_restart should return True when all ranks set valid=True"
    if rc:
        # success, bump timestep to next value and break restart loop
        timestep += 1
        break
    else:
        # someone failed, loop to try another checkpoint
        timestep = 1

# call scr.complete_output without first calling scr.start_output to check that we throw an exception
if test == 4:
    scr.complete_output(True)
    assert False, "The SCR library should have called MPI_Abort if complete_output is called before start_output"

# work loop to execute new timesteps in the job
laststep = timestep + 2
while timestep < laststep:
    #####
    # do work ...
    #####

    # save checkpoint if needed
    rc = scr.need_checkpoint()
    assert rc is True, "scr.need_checkpoint should always return True since SCR_CHECKPOINT_INTERVAL=1"
    if rc:
        # define name of checkpoint to be something like "timestep_10"
        name = 'timestep_' + str(timestep)

        # start checkpoint phase
        rc = scr.start_output(name, scr.FLAG_CHECKPOINT)
        assert rc is None, "scr.start_output should return None"

        # define name of checkpoint file for this timestep and rank
        fname = 'ckpt_' + str(timestep) + '_' + str(rank) + '.txt'

        # register our checkpoint file with scr, and get path to write file from scr
        newfname = scr.route_file(fname)
        assert type(newfname) is str, "scr.route_file should return a string"
        print("output route: ", fname, "-->", newfname)

        # write checkpoint file
        valid = 1
        try:
            with open(newfname, "w") as f:
                f.write('time=' + str(timestep) + "\n")
                f.write('rank=' + str(rank) + "\n")
        except:
            # failed to write file
            valid = 0

        # complete the checkpoint phase
        rc = scr.complete_output(valid)
        assert rc is True, "scr.complete_output should return True if all procs set valid=True"
        print("complete output: ", rc)

    # break timestep loop early if scr informs us to exit
    rc = scr.should_exit()
    assert type(rc) is bool, "scr.should_exit returns a bool"
    if rc:
        break

    # otherwise go on to next timestep
    timestep += 1

# test drop and delete
if test == 3:
    # test dropping a known checkpoint
    rc = scr.drop('timestep_1')
    assert rc is None, "Failed to drop timestep_1"

    # test dropping a known checkpoint
    rc = scr.delete('timestep_2')
    assert rc is None, "Failed to delete timestep_2"

    # test dropping an unknown checkpoint
    # TODO: this succeeds since SCR_Drop always succeeds
    # it perhaps could be changed to return an error, and have python throw an exception
    rc = scr.drop('timestep_doesnotexist')
    assert rc is None, "Failed to drop timestep_doesnotexist"

# shut down library and flush cached datasets
rc = scr.finalize()
assert rc is None, "scr.finalize should return None"
