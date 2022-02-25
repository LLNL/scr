#! /bin/bash

# https://github.com/google/yapf
# do ->
# pip install yapf
# then

yapf --style='{based_on_style: pep8, indent_width: 2}' --in-place --recursive scrjob/
