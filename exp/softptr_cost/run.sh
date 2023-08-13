#!/bin/bash

tests=( softptr_read_small softptr_read_large softptr_write_small softptr_write_large unique_ptr_read_small unique_ptr_read_large unique_ptr_write_small unique_ptr_write_large )

for test in "${tests[@]}"
do
    echo "Running" build/$test
    ./build/$test
    sleep 1
done