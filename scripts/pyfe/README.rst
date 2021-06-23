Python equivalents of perl/bash scripts in scr/scripts/*

(Initial / tentative stage)

Using the setup.py in this directory, we can do:

python3 -m venv venv
source venv/bin/activate
pip3 install -e .
python3 launch.py

Where launch.py in this folder could import scr_run from pyfe.scr_run and call scr_run()

