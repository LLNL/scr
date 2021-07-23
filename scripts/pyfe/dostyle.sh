#! /bin/bash

yapf --style='{based_on_style: pep8, indent_width: 2}' --in-place --recursive pyfe/
