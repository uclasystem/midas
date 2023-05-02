#!/bin/bash

taskset -c 24,26,28,30 \
    ./storage_server \
    2>&1 | tee midas.log
