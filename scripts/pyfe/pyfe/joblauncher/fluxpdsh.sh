#! /bin/bash

myrank=$(flux getattr rank)
#myrank=$HOSTNAME
theoutput="$($@)"
while IFS= read -r line; do
  echo $(echo $line | sed 's/^/'"$myrank"': /') ;
done <<< "$theoutput"
