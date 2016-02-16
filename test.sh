#!/usr/bin/env bash

cd #location

for d in */*.xml ; do
	echo "Checking the xml file in $d directory for the contol characters"
	perl -i -pe 's/[[:cntrl:]]//g' $d;
done