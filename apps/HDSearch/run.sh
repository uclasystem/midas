#!/bin/bash

memratio=0.5

# numactl --cpunodebind=0 --membind=0 \
taskset -c 1,3,5,7,9,11,13,15,17,19,21,23 \
    ./bin/hdsearch $memratio 2>&1 | tee midas.log
