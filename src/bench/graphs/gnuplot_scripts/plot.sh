#!/bin/sh
ssh demo@10.196.1.90 'cd silliconDB/cmake_build/src/exec && echo demo1ldom@99 | sudo -S ./util_parse.sh'
scp demo@10.196.1.90:~/silliconDB/cmake_build/src/exec/*.dat .
gnuplot utilization
open util.eps
