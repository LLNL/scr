These testing scripts that are included with the SCR release
are not currently production-ready and are intended for our internal use. 
However, we include them in case it helps others with testing their
installations of SCR.  Also, these scripts are written for use on LLNL 
systems, and will likely require editing to run on other systems. 
Because of these two factors, we expect users to need our help running and 
adapting these scripts. Please see the SUPPORT section below for our 
contact information.

OVERVIEW
---------------------------------
There are 2 ways to use the testing scripts in this directory: 
fully automated testing and interactive testing. 

FULLY AUTOMATED TESTING
---------------------------------
To use fully automated testing, simply run the
   BUILD_AND_TEST
bash script with a single argument, the file to which you want output written.
The BUILD_AND_TEST script will build SCR and run the TEST python script as 
a batch job, which will parse the output and determine whether or not the 
test was passed. The last thing printed into the output file will be whether 
the test passed or failed. If the output file is a relative path, the path 
will be interpreted relative to the examples directory, found at 
$SCR_INSTALL/share/scr/examples

INTERACTIVE TESTING
---------------------------------
To use interactive testing, run the
   BUILD_FOR_TESTING
bash script with a single argument, the maximum length of the interative 
testing session.  The BUILD_FOR_TESTING script will build SCR and open 
an mxterm allocation on 4 nodes for testing.

Testing commands to copy into the interactive testing session can be found in the
   TESTING.sh
and
   TESTING.csh
scripts. These scripts are identical except for language. The TESTING.sh 
script is for bash users, and the TESTING.csh script for csh users.
These commands are not required for interative testing, but if executed in 
the presented order represent the same tests as the automated TEST script.
It is also possible to run the automated TEST python script from an interactive 
testing session.

DISTRIBUTION
---------------------------------

To verify that current version of SCR is ready for distribution, run the DISTRIBUTION.sh script from the SCR root directory:

```
        $ cd /path/to/scr/
        $ ./testing/DISTRIBUTION.sh
```

Note that no environment variables need to be set.
A sucessful and valid distribution will end with the following message:

```
===========================================
scr-1.1.8 archives ready for distribution: 
scr-1.1.8.tar.gz
===========================================
```


SUPPORT
---------------------------------
For help in running or adpating these tests, please email 
scalablecr-discuss@lists.sourceforge.net for assistance.
