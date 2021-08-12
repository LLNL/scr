========================================================
Testing scripts for SCR+pyfe  
========================================================

| The file *runtest.sh* will iterate through all test*.py scripts  
| It can be ran in one of three ways:  
| ``./runtest.sh``  
|   do all test*.py scripts and the bottom portion of runtest.sh  
| ``./runtest.sh scripts``  
|   only do the test*.py scripts, following 1 run of the test_api  
| ``./runtest.sh <word>``  
|   use some other word to only do the bottom of runtest.sh  
|  

========================================================
Usage instructions, All use cases  
========================================================

| These variables at the top of runtest.sh must be set:  
|   launcher="srun"  - options:{srun,jsrun,flux,aprun,mpirun,lrun}  
|   numnodes="2"     - the number of nodes in the allocation  
|   MPICC="mpicc"    - the MPI C compiler to use for compiling test programs  

========================================================
runtest.sh within an interactive allocation  
========================================================

| *SLURM + srun*  
| ``salloc -N2 -ppdebug``  
| ``cd ~/scr/install/bin/pyfe/tests``  
| ``./runtest.sh``  
|  
| *SLURM + flux*  
| ``cd ~/scr/install/bin/pyfe/tests``  
| ``source fluxenv.sh``  
| ``salloc -N2 -ppdebug``  
| ``srun -N2 -n2 --pty flux start``  
| ``./runtest.sh``  
|  
| ``LSF + jsrun``  
| ``bsub -q pdebug -nnodes 2 -Is /usr/bin/bash``  
| ``cd ~/scr/install/bin/pyfe/tests``  
| ``./runtest.sh``  

========================================================
runtest.sh withing a batch script
========================================================

| *SLURM + flux*  
| **Prepare the environment**  
| ``cd ~/scr/install/bin/pyfe/tests``  
| ``source fluxenv.sh``  
| **Setup your submission script**  
| ``#!/usr/bin/bash``  
| ``#SBATCH -N, -J, -t, -p, -A, -o ...``  
| ``#SBATCH --export=ALL``  
| ``#SBATCH --wait-all-nodes=1``  
| ``srun -N 2 -n 2 flux start ./runtest.sh``  
| **With these steps completed, submit the script:**  
| ``sbatch submit.sh``  
|  
