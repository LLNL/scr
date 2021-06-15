#! /usr/bin/env python

from setuptools import setup

setup(
    name='pyfe',
    version='0.0',
    description='SCR Python Front End',
    url='https://github.com/LLNL/scr/',
    author='LLNL',
    author_email='pao@llnl.gov',
    license='BSD 3-clause + addendum',
    packages=['pyfe'],
    install_requires=[#'mpi4py>=2.0',
                      #'numpy',
                      ],

    classifiers=[
        #'Intended Audience :: Science/Research',
        #'License :: OSI Approved :: BSD License',  
        #'Operating System :: POSIX :: Linux',        
        #'Programming Language :: Python :: 3',
    ],
)
