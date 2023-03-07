#/bin/bash

ulimit -n 1024000

rm -rf /dev/shm/*

./bin/daemon_main
