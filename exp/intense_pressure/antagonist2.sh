#!/bin/bash

stt_limit=15360
end_limit=5120

# stt_limit=10240
# end_limit=2048

# stt_limit=12288
# end_limit=2048

nr_steps=2
step_size=$(($(($stt_limit - $end_limit)) / $nr_steps))

mem_limit=$stt_limit

sleep 300

for i in $( seq 1 $nr_steps )
do
    mem_limit=$(($mem_limit - $step_size))
    echo $i, $mem_limit
    ./set_memratio.sh $mem_limit
    sleep 300
done
