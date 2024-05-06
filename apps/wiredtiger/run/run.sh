#!/bin/bash

WTPERF_BIN=../build/bench/wtperf/wtperf

rm -rf WT_TEST
./${WTPERF_BIN} -O ycsb.wtperf 2>&1 | tee midas.log
