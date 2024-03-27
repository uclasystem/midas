#!/bin/bash

tests=( softptr_read_large softptr_write_large unique_ptr_read_large unique_ptr_write_large )

objsizes=( 64 128 256 512 1024 2048 4096 8192 12288 16384 )

partial="true"

rm -rf log
mkdir -p log

for size in "${objsizes[@]}"
do
    echo "Object size: $size"
    for test in "${tests[@]}"
    do
        bytes=$(($size * 1024))
        num_eles=$((40 * 1024 * 1024 / $size))
        sed "s/constexpr static bool kPartialAccess.*/constexpr static bool kPartialAccess = $partial;/g" -i $test.cpp
        sed "s/constexpr static int kNumLargeObjs.*/constexpr static int kNumLargeObjs = $num_eles;/g" -i $test.cpp
        sed "s/constexpr static int kLargeObjSize.*/constexpr static int kLargeObjSize = $bytes;/g" -i $test.cpp
    done
    make -j

    for test in "${tests[@]}"
    do
        log_file="log/$test.log"
        echo "Running" build/$test
        ./build/$test | tee -a $log_file
        sleep 1
    done
done
