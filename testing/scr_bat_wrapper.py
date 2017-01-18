# All rights reserved. This program and the accompanying materials
# are made available under the terms of the BSD-3 license which accompanies this
# distribution in LICENSE.TXT
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
# LICENSE.TXT for more details.
#
# GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
# The Government's rights to use, modify, reproduce, release, perform,
# display, or disclose this software are subject to the terms of the BSD-3
# License as provided in Contract No. B609815.
# Any reproduction of computer software, computer software documentation, or
# portions thereof marked with this legend must also reproduce the markings.
#
# Authors: 
# Stephen Willson <stephen.d.willson@intel.com>
# Christopher Holguin <christopher.a.holguin@intel.com>
#
# (C) Copyright 2015-2016 Intel Corporation.


import unittest
from subprocess import *
import atexit
import time
import re
import os
import sys

hardcoded_global_path = os.getcwd()
hardcoded_global_path+="/"
print("global path to use for scr_prefix: "+hardcoded_global_path)

#the following could be parsed out of
#   /scratch/coral/artifacts/scr-review-caholgu1/latest/scr.conf
#   but, for now, it also works to just append the build number before using it
#   (see below, just before executing setup()
hardcoded_nl_path = "/tmp/automation/scr/"

#global array to contain all the spawned proc info
PROCS = []
#global for build number: (it is set before using any of the functions)
build_number=""

#should be no need to modify these, just the names of the output files:
hardcoded_hl_filename = "scr_hostlist.txt"
hardcoded_testoutput_filename = "test_output.txt"
hardcoded_pidfile_name = "scrpidfile"

#these globals will be set just before setup() is called
hardcoded_combined_log_file = ""
hardcoded_hl_complete_filename = ""
hardcoded_pidfile = ""
hardcoded_nl_output_file = ""

@atexit.register
def kill_subprocesses():
    try:
        for proc in PROCS:
            if proc.poll() is None:
                proc.kill()

    except OSError as err:
        print("os error handled: "+str(err))

    try:
        output_file = open(hardcoded_pidfile, 'r+')
        output = output_file.read().strip()
        output_file.close()
        Popen(["kill", "-9", output])
    except IOError as err:
        print("issue opening pidfile: "+str(err))

    cppr_proc = Popen(["./stop_cppr.sh", path_to_build,  host_list, hardcoded_global_path])
    cppr_proc.wait()
    

class TestSCR_BAT(unittest.TestCase):

    def test_scr_bat(self):

        PROCS.append(Popen(["./submit_command.sh", path_to_build, host_list, hardcoded_global_path, hardcoded_nl_output_file, hardcoded_combined_log_file]))
        counter = 0
        # wait for the proces to return
        PROCS[1].wait()
        if PROCS[1].returncode != 0:
            print ("SCR_BAT FAILED WITH: "+str(PROCS[1].returncode))
            #need to assert
            self.assertEqual(PROCS[1].returncode, 0, "orterun process exited with non zero")
        else:
            try:
                #read output file, check for SCR_BAT_SUCCESS or SCR_BAT_FAILURE
                output_file = open(hardcoded_combined_log_file, 'r+')
                output = output_file.read()
                output_file.close()

                #check for failure in the file
                completion_statements = \
                    re.findall("SCR_BAT_FAILURE", output)
                if(len(completion_statements) >= 1):
                    internal_output = re.findall("SCR_BAT test \d+.* failed",
                                                 output)
                    for x in internal_output:
                        print(x)
                    self.assertTrue(False)
                    return

                #check for success in the file
                completion_statements = \
                    re.findall("SCR_BAT_SUCCESS", output)
                self.assertGreaterEqual(len(completion_statements), 1,
                                        "scr_bat could not determine success "
                                        "or failure\n")

            except IOError as err:
                self.assertFalse(True, "couldn't access log file: "+str(err))


def write_hostfile():

    f = open(hardcoded_hl_complete_filename,'w')
    #convert to array
    host_list_array = host_list.split(',')
    for node in host_list_array:
        #format: <node> slots:<n>
        f.write(node + " slots=10\n") # python will convert \n to os.linesep

    f.close() # you can omit in most cases as the destructor will call it

def __setup():

    print("SCR BAT setup begin.")

    print("Launching orte-dvm on all nodes")

    PROCS.append(Popen(["./launch_dvm.sh", path_to_build,  host_list, hardcoded_global_path]))
    time.sleep(3)

    cppr_proc = Popen(["./launch_cppr.sh", path_to_build,  host_list, hardcoded_global_path])
    cppr_proc.wait()
    if cppr_proc.returncode != 0:
        print("failed launching cppr daemon")
        exit(1)
    print("SCR BAT setup end.")


def __teardown():

    print("SCR BAT teardown begin.")

    kill_subprocesses()

    print("SCR BAT teardown end.")


if len(sys.argv) != 3:
    print("usage is: " + sys.argv[0] + " <path to build> <csv hostlist>")
    exit(0)

host_list = sys.argv[2]
print("Hosts: " + host_list)
path_to_build = sys.argv[1]

#need to remove trailing slash if there is one:
if path_to_build[len(path_to_build)-1] == "/":
    path_to_build = path_to_build[:len(path_to_build)-1]
try:
    path_to_build = os.readlink(path_to_build)
except (IOError, OSError) as err:
    print("the path to SCR build is not /latest symlink, so scr_bat will use it exactly as you've provided\n")

#append '/' to the end because the scripts expect it this way
path_to_build = path_to_build + "/"
print("path is: " + path_to_build)

#determine if we're using latest or a particular build number
output_list = re.findall(r"/\d+/", path_to_build)
if len(output_list) > 1:
    print("unexpected path to scr build (too many numbers), exiting: '"+path_to_scr+"'\n")
    exit(1)
if len(output_list) == 1:
    for i in output_list:
        build_number = str(i.replace('/',''))
else :
    output_list = re.findall("latest", path_to_scr)
    if len(output_list) == 1:
        print("must pass a path including the build number (not latest) to these scripts")
        exit(1)
    else :
        print("unexpected path to scr build (too many 'latest' strings): '"+path_to_scr+"'\n")
        exit(1)

#derived complete paths to files
hardcoded_combined_log_file = hardcoded_global_path + hardcoded_testoutput_filename
hardcoded_hl_complete_filename = hardcoded_global_path + hardcoded_hl_filename
hardcoded_pidfile = hardcoded_global_path + hardcoded_pidfile_name
hardcoded_nl_output_file = hardcoded_nl_path + build_number + "/" + hardcoded_testoutput_filename

#now execute setup, test case, and then teardown
write_hostfile()
__setup()
SUITE = unittest.TestLoader().loadTestsFromTestCase(TestSCR_BAT)
unittest.TextTestRunner(verbosity=2).run(SUITE)
__teardown()


