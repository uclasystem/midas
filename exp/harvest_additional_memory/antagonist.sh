#!/bin/bash

stt_limit=5120
end_limit=15360

nr_steps=2
duration=300
step_size=$((($end_limit - $stt_limit) / nr_steps))

mem_limit=$stt_limit
echo 0, $mem_limit
./set_memratio.sh $mem_limit
sleep 600

for i in $( seq 1 $nr_steps )
do
    mem_limit=$(($mem_limit + $step_size))
    echo $i, $mem_limit
    ./set_memratio.sh $mem_limit
    sleep $duration
done
