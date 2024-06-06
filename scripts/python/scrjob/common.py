"""Defines for common methods shared across scripts."""

import os
import sys
import argparse
import inspect
from subprocess import Popen, PIPE
import shlex

from scrjob import config


def tracefunction(frame, event, arg):
    """This method provides a hook for tracing python calls.

    Usage:  sys.settrace(common.tracefunction)
    Prints: filename:function:linenum -> event

    The method below will be called on events as scripts execute.
    The print message in the try block attempts to print the filename.
    If the filename could not be obtained, an exception is thrown.

    This output is very verbose. Filters could be added for event type,
    and print messages could be suppressed when the filename is not known.
    """
    try:
        print(
            inspect.getfile(frame).split('/')[-1] + ':' +
            str(frame.f_code.co_name) + '():' + str(frame.f_lineno) + ' -> ' +
            str(event))
    except:
        print(
            str(frame.f_code.co_name) + '():' + str(frame.f_lineno) + ' -> ' +
            str(event))


def interpolate_variables(varstr):
    """This method will expand a string, including paths.

    This method allows interpretation of path strings such as:
    ~ . ../ ../../

    Environment variables in the string will also be expanded.
    The input need not be a path.
    """
    if varstr is None or len(varstr) == 0:
        return ''

    # replace ~ and . symbols from front of path
    if varstr[0] == '~':
        varstr = '$HOME' + varstr[1:]
    elif varstr.startswith('..'):
        topdir = '/'.join(os.getcwd().split('/')[:-1])
        varstr = varstr[3:]
        while varstr.startswith('..'):
            topdir = '/'.join(topdir.split('/')[:-1])
            varstr = varstr[3:]
        varstr = topdir + '/' + varstr
    elif varstr[0] == '.':
        varstr = os.getcwd() + varstr[1:]

    if varstr[-1] == '/' and len(varstr) > 1:
        varstr = varstr[:-1]

    return os.path.expandvars(varstr)


def scr_prefix():
    """This method will return the prefix for SCR.

    If the environment variable is not set, it will return the current
    working directory. Allows the environment variable to contain
    relative paths by returning the string after passing through
    interpolate_variables
    """
    prefix = os.environ.get('SCR_PREFIX')
    if prefix is None:
        return os.getcwd()

    # tack on current working dir if needed
    # don't resolve symlinks
    # don't worry about missing parts, the calling script calling might create it
    return interpolate_variables(prefix)


def runproc(argv,
            wait=True,
            getstdout=False,
            getstderr=False,
            verbose=False,
            shell=False):
    """This method will execute a command using subprocess.Popen.

    Required argument
    -----------------
    argv        a string or a list, the command to be ran

    Returns
    -------
    tuple       2 values are returned as a tuple, the values depend on optional parameters
                When getting output (waiting, default):
                  The first return value is the output, the second is the returncode
                  If only getting stdout OR stderr, the first element will be a string
                  If getting stdout AND stderr, the first element will be a list,
                  where the first element of the list is stdout and the second is stderr
                  The return code will be an integer
                When not getting output, when wait = False:
                  When wait is False, this method immediately returns after calling Popen.
                  The Popen object will be both the first and second return values
                On ERROR:
                  On error this method returns None, None
                  This has an exception if getstderr is True, then the first return value
                  will contain stderr in the expected position, and the second value will be None

    Optional parameters
    -------------------
      All optional parameters are True/False boolean values
      The parameters getstdout and getstderr require waiting for the program to complete,
      and should not be used in combination with wait=True

    wait        If wait is False this method returns the Popen object as both elements of the tuple
    getstdout   If getstdout is True then this method will include stdout in the first return value
    getstderr   If getstderr is True then this method will include stderr in the first return value
    verbose     If verbose is True then this method will print the command to be executed
    shell       If shell is True, then argv is transformed into -> 'bash -c ' + shlex.quote(argv)

    The shell option is passed into the Popen call, and is supposed to prepend commands (?) with
      '/bin/sh -c', although when I tried simply using shell=True for 'which pdsh' I had errors.
    """
    if shell:
        if type(argv) is list:
            argv = ' '.join(argv)

        # following the security recommendation from subprocess.Popen:
        # turn the command into a shlex quoted string
        argv = 'bash -c ' + shlex.quote(argv)

    # allow caller to pass command as a string, as in:
    #   "ls -lt" rather than ["ls", "-lt"]
    elif type(argv) is str:
        argv = shlex.split(argv)

    if len(argv) < 1:
        return None, None

    try:
        # if verbose, print the command we will run to stdout
        if verbose:
            if type(argv) is list:
                print(" ".join(argv))
            else:
                print(argv)

        runproc = Popen(argv,
                        bufsize=1,
                        stdin=None,
                        stdout=PIPE,
                        stderr=PIPE,
                        shell=shell,
                        universal_newlines=True)

        if wait == False:
            return runproc, runproc

        if getstdout == True and getstderr == True:
            output = runproc.communicate()
            return output, runproc.returncode

        if getstdout == True:
            output = runproc.communicate()[0]
            return output, runproc.returncode

        if getstderr == True:
            output = runproc.communicate()[1]
            return output, runproc.returncode

        runproc.communicate()
        return None, runproc.returncode

    except Exception as e:
        print('runproc: ERROR: ' + str(e))

        if getstdout == True and getstderr == True:
            return ['', str(e)], None

        if getstdout == True:
            return '', None

        if getstderr == True:
            return str(e), None

        return None, None


# pipeproc works as runproc above, except argvs is a list of argv lists
# the first subprocess is opened and from there stdout is chained to stdin
# values returned (returncode/pid/stdout/stderr) will be from the final process
def pipeproc(argvs, wait=True, getstdout=False, getstderr=False):
    """This method is an extension of the above runproc method.

    This is essentially duplicated code from the runproc.  ### TODO :
    This functionality could be put into runproc, increasing   the
    complexity slightly, or it could be kept separate.

    This method will loop through search argv in a list of argvs. The
    output of each previous 'runproc' will be piped to the next as
    stdin. Any return values will come from the result of the final
    Popen command
    """
    if len(argvs) < 1:
        return None, None

    if len(argvs) == 1:
        return runproc(argvs[0], wait, getstdout, getstderr)

    try:
        # split command into list if given as string
        cmd = argvs[0]
        if type(cmd) is str:
            cmd = shlex.split(cmd)

        nextprog = Popen(cmd,
                         bufsize=1,
                         stdin=None,
                         stdout=PIPE,
                         stderr=PIPE,
                         universal_newlines=True)

        for i in range(1, len(argvs)):
            # split command into list if given as string
            cmd = argvs[i]
            if type(cmd) is str:
                cmd = shlex.split(cmd)

            pipeprog = Popen(cmd,
                             stdin=nextprog.stdout,
                             stdout=PIPE,
                             stderr=PIPE,
                             bufsize=1,
                             universal_newlines=True)
            nextprog.stdout.close()
            nextprog = pipeprog

        if wait == False:
            return nextprog, nextprog.pid

        if getstdout == True and getstderr == True:
            output = nextprog.communicate()
            return output, nextprog.returncode

        if getstdout == True:
            output = nextprog.communicate()[0]
            return output, nextprog.returncode

        if getstderr == True:
            output = nextprog.communicate()[1]
            return output, nextprog.returncode

        return None, nextprog.returncode

    except Exception as e:
        print('pipeproc: ERROR: ' + str(e))

        if getstdout == True and getstderr == True:
            return ['', str(e)], 1

        if getstdout == True:
            return '', 1

        if getstderr == True:
            return str(e), 1

        return None, 1


def choose_bindir():
    """Determine appropriate location of binaries.

    Testing using "make test" or "make check" must operate based on the
    contents of the build directory.

    If the script is being run from the install directory, then return
    X_LIBEXECDIR as defined by cmake so installed scripts use installed
    binaries.  Otherwise, determine the appropriate build directory.
    """
    # Needed to find binaries in build dir when testing
    parent_of_this_module = '/'.join(
        os.path.realpath(__file__).split('/')[:-1])

    if config.X_LIBEXECDIR in parent_of_this_module:
        bindir = config.X_LIBEXECDIR  # path to install bin directory
    else:
        bindir = os.path.join(config.CMAKE_BINARY_DIR + "/src/")

    return bindir


if __name__ == '__main__':
    """This script allows being called as a standalone script.

    This is meant for testing purposes, to directly call methods.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--interpolate',
                        metavar='<variable>',
                        type=str,
                        help='Interpolate a variable string.')
    parser.add_argument('--prefix',
                        action='store_true',
                        help='Print the SCR prefix.')
    parser.add_argument('--runproc',
                        nargs=argparse.REMAINDER,
                        help='Launch process with arguments')
    parser.add_argument(
        '--pipeproc',
        nargs=argparse.REMAINDER,
        help=
        'Launch processes and pipe output to other processes. (separate processes with a colon)'
    )

    args = parser.parse_args()

    if args.interpolate:
        print('interpolate_variables(' + args.interpolate + ')')
        print('  -> ' + str(interpolate_variables(args.interpolate)))

    if args.prefix:
        print('scr_prefix()')
        print('  -> ' + str(scr_prefix()))

    if args.runproc:
        print('runproc(' + ' '.join(args.runproc) + ')')
        out, returncode = runproc(argv=args.runproc,
                                  getstdout=True,
                                  getstderr=True)
        print('  process returned with code ' + str(returncode))
        print('  stdout:')
        print(out[0])
        print('  stderr:')
        print(out[1])

    if args.pipeproc:
        printstr = 'pipeproc( '
        argvs = []
        argvs.append([])
        i = 0
        for arg in args.pipeproc:
            if arg == ':':
                i += 1
                argvs.append([])
                printstr += '| '
            else:
                argvs[i].append(arg)
                printstr += arg + ' '
        print(printstr + ')')
        out, returncode = pipeproc(argvs=argvs, getstdout=True, getstderr=True)
        print('  final process returned with code ' + str(returncode))
        if out is not None:
            print('  stdout:')
            print(out[0])
            print('  stderr:')
            print(out[1])
