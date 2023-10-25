from mpi4py import MPI
import scr

rank = MPI.COMM_WORLD.rank

# assume we'll start the job from beginning (timestep 1)
timestep = 1

# optionally set and get SCR parameters with scr.config() before scr.init()
print("SCR_DEBUG:", scr.config("SCR_DEBUG"))
scr.config("SCR_DEBUG=1")
print("SCR_DEBUG:", scr.config("SCR_DEBUG"))

# configure scr.need_checkpoint() to return True every second time it's called
scr.config("SCR_CHECKPOINT_INTERVAL=2")

# initialize library, rebuild cached datasets, fetch latest checkpoint
scr.init()

# attempt to restart from a previous checkpoint
while True:
    # see if we have a restart to load
    if not scr.have_restart():
        # no restart to load, give up
        break

    # got a restart to load, start restart phase and get its name
    name = scr.start_restart()
    print("restart name: ", name)

    # extract timestep value from name like "timestep_10"
    timestep = int(name[9:])

    # build file name for this timestep and rank
    fname = 'ckpt_' + str(timestep) + '_' + str(rank) + '.txt'

    # ask scr for path from which to read our file
    newfname = scr.route_file(fname)
    print("restart route: ", fname, "-->", newfname)

    # read checkpoint file
    valid = True
    try:
        with open(newfname, "r") as f:
            data = f.readlines()
            print(str(data))
    except:
        # failed to read file
        valid = False

    # test: fake a bad file on rank 0 for timestep 6
    if rank == 0 and timestep == 6:
        valid = False

    # check whether everyone read their checkpoint file successfully
    if scr.complete_restart(valid):
        # success, bump timestep to next value and break restart loop
        timestep += 1
        break
    else:
        # someone failed, loop to try another checkpoint
        timestep = 1

# work loop to execute new timesteps in the job
laststep = timestep + 2
while timestep < laststep:
    #####
    # do work ...
    #####

    # save checkpoint if needed
    if scr.need_checkpoint():
        # define name of checkpoint to be something like "timestep_10"
        name = 'timestep_' + str(timestep)

        # start checkpoint phase
        scr.start_output(name, scr.FLAG_CHECKPOINT)

        # define name of checkpoint file for this timestep and rank
        fname = 'ckpt_' + str(timestep) + '_' + str(rank) + '.txt'

        # register our checkpoint file with scr, and get path to write file from scr
        newfname = scr.route_file(fname)
        print("output route: ", fname, "-->", newfname)

        # write checkpoint file
        valid = True
        try:
            with open(newfname, "w") as f:
                f.write('time=' + str(timestep) + "\n")
                f.write('rank=' + str(rank) + "\n")
        except:
            # failed to write file
            valid = False

        # complete the checkpoint phase
        rc = scr.complete_output(valid)
        print("complete output: ", rc)

    # break timestep loop early if scr informs us to exit
    if scr.should_exit():
        break

    # otherwise go on to next timestep
    timestep += 1

# shut down library and flush cached datasets
scr.finalize()
