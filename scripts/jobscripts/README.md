# Example SCR batch job scripts
The scripts in this directory demonstrate how to integrate SCR in a batch job script.
One may use them as a drop in substitute for the corresponding job launcher.
They serve as examples for users to copy and customize for their own needs.
The scripts are installed to ``share/scr/jobscripts`` in an SCR installation.

In particular, the scripts show how to use the SCR scavenge and scalable restart features.
For these to work, one must configure the allocation to continue through node failures.
The example job scripts include a directive to do this, which is specific to the resource manager.

## Single launch with scavenge
These scripts launch the run once and then run ``scr_postrun`` to scavenge any cached dataset.

- ``scr_srun.sh``  - Launch with ``srun`` in a SLURM allocation
- ``scr_jsrun.sh`` - Launch with ``jsrun`` in an LSF allocation
- ``scr_flux_run.sh`` - Launch with ``flux run`` in a Flux allocation

## Multiple launch with scalable restart and scavenge
The scripts that end in ``_loop`` relaunch a run multiple times within an allocation.
This enables one to request extra (spare) nodes in their allocation and then benefit from the SCR scalable restart.

The script launches the run up to a maximum of 5 times.
After each run, the script checks whether the job should exit.
The script exits if any of the following hold:
- the job has completed (the application called ``SCR_Finalize``)
- the user has specified an active ``scr_halt`` condition
- there is insufficient time remaining, based on ``SCR_HALT_SECONDS`` and the allocation end time
- there are insufficient nodes remaining (too many failed nodes)

If the job can keep going, the script detects and avoids any failed nodes in subsequent launches.
The mechanism to avoid failed nodes depends on the job launch command.

Before the script exits, it runs ``scr_postrun`` to scavenge any cached dataset.

- ``scr_srun_loop.sh`` - Launch with ``srun`` in a SLURM allocation
- ``scr_jsrun_loop.sh`` - Launch with ``jsrun`` in an LSF allocation
- ``scr_flux_run_loop.sh`` - Launch with ``flux run`` in an Flux allocation
