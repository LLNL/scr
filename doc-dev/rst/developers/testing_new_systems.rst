.. _new_systems:

Testing SCR on a New Systems
============================

To verify that SCR works a new system, there are number of steps and failure modes to check. Usually, these tests require close coordination with a system administrator. It often takes 2-3 months to ensure that everything can work.

.. _test_restart_in_place:

Restart in Place
----------------

1. Configure a job allocation so that it does not automatically terminate due to a node failure.  Most resource managers can support this, but they are often not configured that way by default, because most centers don’t care to continue through those kinds of failures.  The only way to know for sure is to get a job allocation, and have a system admin actually kill a node, e.g., by powering it off.  For this, create a job script that sleeps for some time and then prints some messages after sleeping.  Start the job, then kill a node during the sleep phase, and then verify that the print commands after the sleep still execute.
2. Ensure that a running MPI job actually exits and returns control to the job script.  If a node failure causes MPI to hang so that control never comes back to the job script, that’s not going to work.  To do this, you can launch an MPI job that does nothing but runs in a while loop or sleeps for some time.  Then have an admin kill a node during that sleep phase, and verify that some commands in the job script that come after the mpirun still execute.  Again, simply printing a message is often enough.
3. Detect which node has failed.  Is there a way to ask the system software about failed nodes?  If not, or if the system software is too slow to report it, can we run commands to go inspect things ourselves?
Our existing scripts do various tests, like ping each node that is thought to be up, and then if a node responds to a ping, we run additional tests on each node.  There are failure modes where the node will respond to ping, but the SSD or the GPU may have died, so you also need to run tests on those devices to ensure that the node and all of its critical components are healthy.
4. Verify that you can steer the next job launch to only run on healthy nodes.  There are two things here.  First is to avoid the bad nodes in the next MPI job.  For example, in SLURM one can use the -w option to specify the target node set, and with mpirun, there is often a hostfile that one can create.  For CORAL, jobs are launched with jsrun, and that command has its own style of hostfile that we need to generate.  The second thing is to verify that the system supports a second MPI job to run in a broken allocation after a first MPI job was terminated unexpectedly.
5. Finally verify the complete cycle work with a veloc checkpoint.
   a. Allocate 5 nodes
   b. Write a single job script that launches an MPI job on to the first 4 nodes, saves a veloc checkpoint, then spins in a loop
   c. Have an admin kill one of those 4 nodes
   d. Verify that the MPI job exits, the scripting detects the down node and builds the command to avoid it in the next launch
   e. Launch a new MPI job on the remaining 4 healthy nodes, verify that job rebuilds and restarts from latest veloc checkpoint

.. _test_node_health:

Node Health Checks
------------------

1. Ask resource manager if it knows of any down nodes if there is a way to do that.  This helps us immediately exclude nodes the system already knows to be down.  SLURM can do this, though it may take time to figure it out (5-10 minutes).
2. Attempt to ping all nodes that the resource manager thinks are up or simply all nodes if resource manager can’t tell us anything.  Exclude any nodes that fail to respond. Some systems are not configured to allow normal users to ping the compute nodes, so this step may be skipped.
3. Try to run a test on each node that responds to the `ping`, using `pdsh` if possible, but `aprun` or other system launchers if `pdsh` doesn’t work.  The tests one needs to run can vary.  One might simply run an `echo` command to print something.  On a system with GPUs, you might want to run a test on the GPUs to verify they haven’t gone out to lunch.  On a with SSDs, the SSDs would fail in various ways. This required the following checks:
   a. run `df` against the SSD mount point and check that the total space looks right (a firmware bug caused some drives to lose track of their capacity)
   b. ensure that the mount point was listed as writable.  Sometimes the bits would turn off (another firmware bug)
   c. try to touch and delete a file and verify that works.  Some drives looked ok by the first two checks, but you couldn’t actually create a file (a still different firmware bug)
4. Additionally, the `SCR_EXCLUDE_NODES` environment variable allows a user to list nodes they want to avoid.  This is a catch-all so that a user could make progress through a weekend in case they found a problematic node that our other checks didn’t pick up.  That came into existence when a user stumbled into a node with a bad CPU one weekend that would generate segfaults.  We didn’t have any checks for it, nor did the system, so the jobs kept getting scheduled to the bad node, even though it was clear to the user that the node was bogus.
5. Finally, the SCR scripting was written so that once a node is ruled out for any of the above, we always keep it out for the remainder of the job allocation.  That way we’d avoid retrying problematic nodes that would come and go.

This logic is often system specific and is stored in the `scr_list_down_nodes` script.
