#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SRC_DIR=$( cd -- $SCRIPT_DIR/.. &> /dev/null && pwd )

mem_mb=$1
mem_limit=$(($mem_mb * 1024 * 1024))

echo "Set memory limit for Midas to $mem_mb MB"
echo $mem_limit > ${SRC_DIR}/config/mem.config
