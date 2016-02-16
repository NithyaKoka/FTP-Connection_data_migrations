#!/usr/bin/env bash

cd /data0/tmp/vk_data/  # directory where your data is..

for d in */*.xml ; do
        echo "Checking the xml file $d for the contol characters"
        perl -i -pe 's/[[:cntrl:]]//g' $d;
done