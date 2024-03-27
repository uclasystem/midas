#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SRC_DIR=$( cd -- $SCRIPT_DIR/.. &> /dev/null && pwd )

ulimit -n 1024000
rm -rf /dev/shm/*
${SRC_DIR}/bin/daemon_main
