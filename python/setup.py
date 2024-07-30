from distutils.core import setup

# https://packaging.python.org/guides/distributing-packages-using-setuptools/#setup-py

# text for long_description in markdown
longtext = """Python module for the SCR library (libscr.so). For usage: ``python -c 'import scr; help(scr)'``"""

setup(
    name='scr',
    version='3.1.0',
    author='LLNL',
    author_email='moody20@llnl.gov',
    url='https://github.com/llnl/scr',
    description='Scalable Checkpoint / Restart (SCR) Library',
    long_description=longtext,
    #  long_description_content_type='text/markdown',
    keywords='scalable, checkpoint, restart, mpi, scr',
    py_modules=['scr'],
    #  install_requires=['cffi'],
    platforms=['posix'],
    license='BSD 3-clause + addendum',
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: BSD License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 3',
    ],
)
