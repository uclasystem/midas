#!/bin/bash

mem_mb=$1
mem_limit=$(($mem_mb * 1024 * 1024))

echo $mem_limit > ../../config/mem.config
