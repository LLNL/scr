#! /bin/bash
if [ "${FLUXTESTSVPATH}" != "" ]; then
  export PATH=${FLUXTESTSVPATH}
  unset FLUXTESTSVPATH
fi
if [ "${FLUXTESTSVLDPATH}" != "" ]; then
  export LD_LIBRARY_PATH=${FLUXTESTSVLDPATH}
  unset FLUXTESTSVLDPATH
fi
