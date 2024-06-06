import sys
import argparse

from scrjob.jobenv import JobEnv

if __name__ == '__main__':
    """This is an external driver to determine control and cache directories."""

    parser = argparse.ArgumentParser()
    parser.add_argument('-b',
                        '--base',
                        action='store_true',
                        default=False,
                        help='List base portion of cache/control directory')
    parser.add_argument('-p',
                        '--prefix',
                        default=None,
                        metavar='<id>',
                        type=str,
                        help='Specify the prefix directory.')
    parser.add_argument('dirtype',
                        choices=['control', 'cache'],
                        metavar='<control | cache>',
                        nargs='?',
                        default=None,
                        help='Specify the directory to list.')

    args = parser.parse_args()

    jobenv = JobEnv(prefix=args.prefix)

    if args.dirtype == 'cache':
        dirs = jobenv.dir_cache(base=args.base)
    elif args.dirtype == 'control':
        dirs = jobenv.dir_control(base=args.base)
    else:
        print('One of [control, cache] must be specified.')
        sys.exit(1)

    print(' '.join(dirs))
